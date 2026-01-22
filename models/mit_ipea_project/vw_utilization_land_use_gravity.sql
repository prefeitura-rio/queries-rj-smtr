WITH buffer AS (
    SELECT *
    FROM `rj-smtr-dev`.mit_ipea_project.origin_destination_land_use_res9_power1
    WHERE dist
)

passengers AS (SELECT h3_gps.as_at,
                           COALESCE(origin_destination.card_id, origin_destination1.card_id)                     AS card_id,
                           COALESCE(origin_destination.destination_time1,
                                    origin_destination1.destination_time1)                                       AS destination_time1,
                           h3_gps.vehicle_id,
                           h3_gps.service,
                           h3_gps.mode,
                           h3_gps.tile_id,
                           h3_gps.tile_entry_time,
                           h3_gps.tile_exit_time,
                           CASE
                               WHEN origin_destination.fraction_on_board IS NULL THEN 1
                               WHEN origin_destination.destination_time1 > h3_gps.tile_entry_time THEN 1
                               WHEN origin_destination.fraction_on_board IS NOT NULL
                                   THEN origin_destination.fraction_on_board
                               END                                                                               AS fraction_on_board

                    FROM `rj-smtr-dev`.mit_ipea_project.h3_gps_res9 AS h3_gps

                             -- JOIN 1
                             LEFT JOIN `rj-smtr-dev`.mit_ipea_project.origin_destination_land_use_res9_power1 AS origin_destination
                                       ON h3_gps.as_at = origin_destination.as_at -- Same date
                                           AND RIGHT(h3_gps.vehicle_id, 5) =
                                               origin_destination.transaction_vehicle_id -- Same vehicle
                                           -- On bus cases:
                                           -- 2. Getting off (destination_time_1 = tile_entry_time)
                                           AND origin_destination.destination_time1 = h3_gps.tile_entry_time
                        -- JOIN 2
                             LEFT JOIN (SELECT as_at,
                                               card_id,
                                               transaction_vehicle_id,
                                               transaction_time,
                                               MIN(destination_time1) AS destination_time1
                                        FROM `rj-smtr-dev`.mit_ipea_project.origin_destination_land_use_res9_power1
                                        GROUP BY as_at, card_id, transaction_vehicle_id, transaction_time) AS origin_destination1
                                       ON h3_gps.as_at = origin_destination1.as_at -- Same date
                                           AND RIGHT(h3_gps.vehicle_id, 5) =
                                               origin_destination1.transaction_vehicle_id -- Same vehicle
                                           -- On bus cases:
                                           -- 1. Getting on (transaction time between tile time)
                                           -- CASE 1
                                           AND
                                          origin_destination1.transaction_time BETWEEN h3_gps.tile_entry_time AND h3_gps.tile_exit_time
                    --AND h3_gps.tile_entry_time > origin_destination1.transaction_time
                    --AND h3_gps.tile_entry_time < origin_destination1.destination_time1

                    WHERE h3_gps.as_at BETWEEN '2023-03-01' AND '2023-06-30'
                    -- Buffer
                    --AND distance_from_next_transaction <= 2000
                    --AND h3_gps.vehicle_id = 'A41190'


                    UNION
                    DISTINCT
--
                    SELECT h3_gps.as_at,
                           origin_destination.card_id AS card_id,
                           origin_destination.destination_time1,
                           h3_gps.vehicle_id,
                           h3_gps.service,
                           h3_gps.mode,
                           h3_gps.tile_id,
                           h3_gps.tile_entry_time,
                           h3_gps.tile_exit_time,
                           1   AS fraction_on_board

                    FROM `rj-smtr-dev`.mit_ipea_project.h3_gps_res9 AS h3_gps


                             LEFT JOIN (SELECT as_at,
                                               card_id,
                                               transaction_vehicle_id,
                                               transaction_time,
                                               MAX(distance_from_next_transaction) AS distance_from_next_transaction,
                                               MIN(destination_time1) AS destination_time1
                                        FROM `rj-smtr-dev`.mit_ipea_project.origin_destination_land_use_res9_power1
                                        GROUP BY as_at, card_id, transaction_vehicle_id, transaction_time) AS origin_destination
                                       ON h3_gps.as_at = origin_destination.as_at -- Same date
                                           AND RIGHT(h3_gps.vehicle_id, 5) =
                                               origin_destination.transaction_vehicle_id -- Same vehicle
                                           -- On bus cases:
                                           -- 3. Riding (tile time is between transaction time and destination time)
                                           -- CASE 3
                                           AND
                                          h3_gps.tile_entry_time > origin_destination.transaction_time AND
                                          h3_gps.tile_entry_time < origin_destination.destination_time1

/*

                    LEFT JOIN `rj-smtr-dev`.mit_ipea_project.origin_destination_land_use_res9_power1 AS origin_destination
                             ON h3_gps.as_at = origin_destination.as_at -- Same date
                                 AND RIGHT(h3_gps.vehicle_id, 5) =
                                     origin_destination.transaction_vehicle_id -- Same vehicle
                                 -- On bus cases:
                                 -- 1. Getting on (transaction time between tile time)
                                 -- 2. Getting off (destination_time_1 = tile_entry_time)
                                 -- 3. Riding (tile time is between transaction time and destination time)

                                 -- CASE 1
                                 AND
                                (origin_destination.transaction_time BETWEEN h3_gps.tile_entry_time AND h3_gps.tile_exit_time
                                    -- CASE 2
                                    OR origin_destination.destination_time1 = h3_gps.tile_entry_time
                                    -- CASE 3
                                    OR
                                 h3_gps.tile_entry_time BETWEEN origin_destination.transaction_time AND origin_destination.destination_time1
                                    )
            -- Timestamp restriction
                                 --AND (
                                 --   destination_time1 <= next_transaction_time
                                 --       OR daily_trip_stage IN ('Last transaction', 'Only transaction')
                                 --   )*/

                    WHERE h3_gps.as_at BETWEEN '2023-03-01' AND '2023-06-30'
    -- Buffer
    --AND distance_from_next_transaction <= 2000
    --AND h3_gps.vehicle_id = 'A41190')

)

SELECT *
    FROM passengers
WHERE card_id = '4ffe61840bc5c81bf699e307a531dc860e4b8ba2296c6c50d1c277086d213a39'
 AND as_at = '2023-06-20'
  --vehicle_id = 'A41190'
ORDER BY tile_entry_time

n_passengers AS (
    SELECT as_at, vehicle_id, service, mode, tile_id, tile_entry_time, tile_exit_time, SUM(fraction_on_board) - 1 AS n

    FROM passengers

    GROUP BY as_at,
             vehicle_id,
             service,
             mode,
             tile_id,
             tile_entry_time,
             tile_exit_time
                      ),

capacity AS (SELECT n_passengers.as_at,
                    n_passengers.vehicle_id,
                    service,
                    n_passengers.mode,
                    tile_id,
                    tile_entry_time,
                    tile_exit_time,
                    n,
                    NULLIF(passengers_sitting, 0)                       AS capacity_sitting,
                    NULLIF(passengers_standing, 0)                      AS capacity_standing,
                    NULLIF(passengers_sitting + passengers_standing, 0) AS capacity_total

             FROM n_passengers

                      LEFT JOIN (SELECT *
                                 FROM `rj-smtr-dev`.mit_ipea_project.vw_vehicle_details
                                 WHERE latest_capture = 'TRUE') AS vw_vehicle_details
                                ON n_passengers.vehicle_id = vw_vehicle_details.vehicle_id),

capacity_percentages AS (SELECT *,
                                ROUND(n / capacity_sitting, 2) AS p_sitting_utilization,
                                ROUND(n / capacity_total, 2)   AS p_utilization
                         FROM capacity)

SELECT
    utilisation_observations.int64_field_0,
    utilisation_observations.time,
    cp.as_at,
       cp.vehicle_id,
       cp.tile_id,
       cp.n,
       cp.p_sitting_utilization,
       cp.p_utilization,
       stop_lat,
       stop_lon,
       centroid,
       ST_DISTANCE(centroid, ST_GEOGPOINT(stop_lon, stop_lat)) AS distance,
       utilisation,
       utilisation_observations.line,
       utilisation_observations.researcher,
       utilisation_observations.location

FROM capacity_percentages AS cp
INNER JOIN `rj-smtr-dev`.mit_ipea_project.utilisation_observations
ON
    RIGHT(cp.vehicle_id, 5) = utilisation_observations.vehicle_id -- Same Vehicle
    AND cp.as_at = utilisation_observations.date -- Same date
    AND EXTRACT(TIME FROM datetime)
        BETWEEN TIME_SUB(tile_entry_time, INTERVAL 5 MINUTE)
        AND TIME_ADD(tile_exit_time, INTERVAL 5 MINUTE) -- Obs time match

LEFT JOIN `rj-smtr-dev`.mit_ipea_project.vw_h3_res9 AS vw_h3
    ON cp.tile_id = vw_h3.tile_id

ORDER BY  int64_field_0


