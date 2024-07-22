{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "as_at",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- CTE 1: Match transaction with next transaction (if final transaction, match with first transaction)
WITH ticketing AS (
    SELECT
        card_id,
        daily_trip_id,
        as_at,
        origin_time AS transaction_time,
        -- Predict destination time using trip chaining
        CASE
            WHEN daily_trip_stage != 'Last transaction' THEN LEAD(origin_time) OVER (PARTITION BY as_at, card_id ORDER BY daily_trip_id)
            ELSE FIRST_VALUE(origin_time) OVER (PARTITION BY as_at, card_id ORDER BY daily_trip_id)
        END AS next_transaction_time,
        vehicle_id as transaction_vehicle_id,
        CASE
            WHEN daily_trip_stage != 'Last transaction' THEN LEAD(vehicle_id) OVER (PARTITION BY as_at, card_id ORDER BY daily_trip_id)
            ELSE FIRST_VALUE(vehicle_id) OVER (PARTITION BY as_at, card_id ORDER BY daily_trip_id)
        END AS next_transaction_vehicle_id,
        daily_trip_stage

    FROM `rj-smtr-dev`.mit_ipea_project.vw_ticketing
    -- In future remove hardcoded date
    WHERE as_at BETWEEN '2023-01-01' AND '2023-01-01'
    --WHERE as_at IN (
    --            '2023-06-02'
    ----, '2023-06-03'
    --           )
),

-- CTE 2: Estimate OD for each transaction
od_prediction AS (

SELECT
    card_id,
    daily_trip_id,
    ticketing.as_at,
    transaction_time,
    transaction_vehicle_id,
    h3_boarding.tile_id             AS boarding_tile,
    h3_boarding.centroid            AS boarding_tile_centriod,
    next_transaction_time,
    next_transaction_vehicle_id,
    h3_next_transaction.tile_id     AS next_transaction_tile_id,
    h3_next_transaction.centroid    AS next_transaction_tile_centroid,

    h3_bus_sitting.tile_entry_time AS destination_time1,
    h3_bus_sitting.tile_exit_time AS destination_time2,
    h3_bus_sitting.tile_id AS destination_tile,
    h3_bus_sitting.centroid AS destination_centroid,
    daily_trip_stage,
    EXP(ln_CMATT30) AS CMATT30,
    ST_DISTANCE(h3_next_transaction.centroid, h3_bus_sitting.centroid) AS distance_from_next_transaction,
    MIN(ST_DISTANCE(h3_next_transaction.centroid, h3_bus_sitting.centroid))
        OVER(PARTITION BY card_id, ticketing.as_at, daily_trip_id) AS exit_prediction,
    ROW_NUMBER() OVER(PARTITION BY card_id, ticketing.as_at, daily_trip_id ORDER BY h3_bus_sitting.tile_entry_time) AS row_num

FROM ticketing

-- NEXT TRANSACTION LOCATION
LEFT JOIN `rj-smtr-dev`.mit_ipea_project.h3_gps AS h3_next_transaction
    ON
        ticketing.next_transaction_vehicle_id   = RIGHT(h3_next_transaction.vehicle_id, 5)
    AND ticketing.as_at                         = h3_next_transaction.as_at
    AND ticketing.next_transaction_time         >= h3_next_transaction.tile_entry_time
    AND ticketing.next_transaction_time         < h3_next_transaction.tile_exit_time
-- BOARDING LOCATION
LEFT JOIN `rj-smtr-dev`.mit_ipea_project.h3_gps AS h3_boarding
    ON
        ticketing.transaction_vehicle_id        = RIGHT(h3_boarding.vehicle_id, 5)
    AND ticketing.as_at                         = h3_boarding.as_at
    -- Between statement cannot be used for tile entry and exit times
    AND ticketing.transaction_time >= h3_boarding.tile_entry_time
    AND ticketing.transaction_time < h3_boarding.tile_exit_time
-- SITTING ON BUS TIME
LEFT JOIN `rj-smtr-dev`.mit_ipea_project.h3_gps AS h3_bus_sitting
    ON
        ticketing.transaction_vehicle_id        = RIGHT(h3_bus_sitting.vehicle_id, 5)
    AND ticketing.as_at                         = h3_bus_sitting.as_at
    AND ticketing.transaction_time NOT BETWEEN h3_bus_sitting.tile_entry_time AND h3_bus_sitting.tile_exit_time
    AND h3_bus_sitting.tile_entry_time BETWEEN ticketing.transaction_time
        AND TIME_ADD(ticketing.transaction_time, INTERVAL 2 HOUR)
LEFT JOIN `rj-smtr-dev`.mit_ipea_project.accessibility_jobs
    ON h3_bus_sitting.tile_id = accessibility_jobs.h3_address

),

min_exit_row AS (
-- May be multiple exit rows
    SELECT *,
           CASE
               WHEN distance_from_next_transaction = exit_prediction THEN 1
               ELSE 0
           END AS exit_row
    FROM od_prediction
    ),

exit_row_num AS (SELECT *,
                        MIN(CASE WHEN exit_row = 1 THEN row_num END)
                            OVER (PARTITION BY as_at, card_id, daily_trip_id) AS min_row_num_with_exit_row_1,
                        MIN(CASE WHEN exit_row = 1 THEN destination_time1 END)
                            OVER (PARTITION BY as_at, card_id, daily_trip_id) AS trip_chaining_destination_time,
                     CASE
                         WHEN distance_from_next_transaction = 0 THEN CMATT30
                        ELSE CMATT30 * (1/distance_from_next_transaction)
                         END AS land_use_score,
                    CASE
                        WHEN
                          TIME_DIFF(
                                destination_time1,
                                MIN(CASE WHEN exit_row = 1 THEN destination_time1 END)
                                    OVER (PARTITION BY as_at, card_id, daily_trip_id),
                                MINUTE) = 0 THEN CMATT30
                        ELSE CMATT30 * (
                            1/
                            POWER(ABS(TIME_DIFF(
                                destination_time1,
                                MIN(CASE WHEN exit_row = 1 THEN destination_time1 END)
                                    OVER (PARTITION BY as_at, card_id, daily_trip_id),
                                MINUTE)
                            ), 0.5)
                            )
                         END AS land_use_score_time
                 FROM min_exit_row
                 ),

land_use_time_estimate AS (
    SELECT *,
          CASE
              WHEN
                  land_use_score_time =
                  MAX(land_use_score_time) OVER (PARTITION BY as_at, card_id, daily_trip_id)
                  THEN 1
              ELSE 0
              END AS exit_row_land_use_time
   FROM exit_row_num
   WHERE row_num BETWEEN min_row_num_with_exit_row_1 - 15 AND min_row_num_with_exit_row_1 + 15
     --AND card_id IN ('000419a63840109bb1f656d75cd2c8f48f8ea90a4a72124f421ad8962094ad12'
    --                 'caf2a50e1ba90fde80690abcc9efc62ca1747702afa20761ecce65d68979b3fd',
    --                 'b76259e138c3d18c1f8e6e198ec91482dd257545a11c3af6addbd8d97d50dde8'
    --   )
--AND daily_trip_id = 3
),

--SELECT *
--FROM land_use_time_estimate
--WHERE exit_row_land_use_time = 1 --xOR row_num = min_row_num_with_exit_row_1
--ORDER BY as_at, card_id, daily_trip_id

origin_destination AS (

SELECT
    card_id,
    as_at,
    transaction_time,
    daily_trip_stage,
    daily_trip_id,
    transaction_vehicle_id,
    boarding_tile,
    destination_time1 AS destination_time1,
    destination_time2 AS destination_time2,
    destination_tile,
    next_transaction_time,
    next_transaction_tile_id,
    CMATT30,
    ST_DISTANCE(boarding_tile_centriod, destination_centroid) AS distance_travelled_linear,
    distance_from_next_transaction,
    exit_prediction AS distance_transit,
    row_num,
    exit_row,
    exit_row_land_use_time
FROM land_use_time_estimate

WHERE exit_row_land_use_time = 1
    AND daily_trip_stage != 'Only transaction'

),

-- Output: UNION ALL
--Transaction with more than one daily trip stage
output AS (

    SELECT
        card_id,
        as_at,
        transaction_time,
        daily_trip_stage,
        daily_trip_id,
        transaction_vehicle_id,
        boarding_tile,
        destination_time1,
        destination_time2,
        destination_tile,
        next_transaction_time,
        next_transaction_tile_id,
        CMATT30,
        distance_travelled_linear,
        distance_from_next_transaction,
        distance_transit,
        row_num,
        exit_row,
        exit_row_land_use_time
    FROM origin_destination

UNION ALL

-- Transaction with only 1 daily trip stage OR where there is no H3_GPS match

SELECT
    DISTINCT
    card_id,
    as_at,
    transaction_time,
    daily_trip_stage,
    daily_trip_id,
    transaction_vehicle_id,
    CAST(NULL AS STRING) AS boarding_tile,
    CAST(NULL AS TIME) AS destination_time1,
    CAST(NULL AS TIME) AS destination_time2,
    CAST(NULL AS STRING) AS destination_tile,
    CAST(NULL AS TIME) AS next_transaction_time,
    CAST(NULL AS STRING) AS next_transaction_tile_id,
    NULL AS CMATT30,
    NULL AS distance_travelled_linear,
    NULL AS distance_from_next_transaction,
    NULL AS distance_transit,
    NULL AS row_num,
    NULL AS exit_row,
    NULL AS exit_row_land_use_time
FROM od_prediction
WHERE daily_trip_stage = 'Only transaction'
  OR distance_from_next_transaction IS NULL
)

SELECT
    *
FROM output

