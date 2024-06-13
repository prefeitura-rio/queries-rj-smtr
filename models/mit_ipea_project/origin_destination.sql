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
    -- Drop transactions with only one tap in that day
    --WHERE daily_trip_stage != 'Only transaction'
    -- In future remove hardcoded date
    WHERE as_at BETWEEN '2023-01-01' AND '2023-06-01'
),

origin_destination AS (

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
    ST_DISTANCE(h3_next_transaction.centroid, h3_bus_sitting.centroid) AS distance_from_next_transaction,
    MIN(ST_DISTANCE(h3_next_transaction.centroid, h3_bus_sitting.centroid))
        OVER(PARTITION BY card_id, ticketing.as_at, daily_trip_id) AS exit_prediction

FROM ticketing

-- NEXT TRANSACTION LOCATION
LEFT JOIN `rj-smtr-dev`.mit_ipea_project.h3_gps AS h3_next_transaction
    ON
        ticketing.next_transaction_vehicle_id   = RIGHT(h3_next_transaction.vehicle_id, 5)
    AND ticketing.as_at                         = h3_next_transaction.as_at
    AND ticketing.next_transaction_time >= h3_next_transaction.tile_entry_time
    AND ticketing.next_transaction_time < h3_next_transaction.tile_exit_time
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

)

--Transaction with more than one daily trip stage
SELECT
    card_id,
    as_at,
    transaction_time,
    daily_trip_stage,
    daily_trip_id,
    transaction_vehicle_id,
    boarding_tile,
    --boarding_tile_centriod,
    MIN(destination_time1) AS destination_time1,
    MIN(destination_time2) AS destination_time2,
    destination_tile,
    --destination_centroid,
    next_transaction_time,
    next_transaction_tile_id,
    AVG(ST_DISTANCE(boarding_tile_centriod, destination_centroid)) AS distance_travelled_linear
FROM origin_destination

WHERE distance_from_next_transaction = exit_prediction
    AND daily_trip_stage != 'Only transaction'

GROUP BY card_id, as_at, transaction_time, daily_trip_stage, daily_trip_id, transaction_vehicle_id, boarding_tile,
         destination_tile, next_transaction_time, next_transaction_tile_id

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
    --boarding_tile_centriod,
    CAST(NULL AS TIME) AS destination_time1,
    CAST(NULL AS TIME) AS destination_time2,
    CAST(NULL AS STRING) AS destination_tile,
    --destination_centroid,
    CAST(NULL AS TIME) AS next_transaction_time,
    CAST(NULL AS STRING) AS next_transaction_tile_id,
    NULL AS distance_travelled_linear
FROM origin_destination
WHERE daily_trip_stage = 'Only transaction'
  OR distance_from_next_transaction IS NULL
