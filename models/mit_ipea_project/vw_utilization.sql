WITH n_passengers AS (SELECT h3_gps.as_at,
                             h3_gps.vehicle_id,
                             h3_gps.service,
                             h3_gps.mode,
                             h3_gps.tile_id,
                             h3_gps.tile_entry_time,
                             h3_gps.tile_exit_time,
                             COUNT(*) - 1 AS n

                      FROM `rj-smtr-dev`.mit_ipea_project.h3_gps

                               LEFT JOIN `rj-smtr-dev`.mit_ipea_project.origin_destination_land_use AS origin_destination
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

                      WHERE h3_gps.as_at BETWEEN '2023-03-01' AND '2023-06-30'
                      AND distance_from_next_transaction <= 2000
 --AND h3_gps.vehicle_id = 'A41190'

                      GROUP BY h3_gps.as_at,
                               h3_gps.vehicle_id,
                               h3_gps.service,
                               h3_gps.mode,
                               h3_gps.tile_id,
                               h3_gps.tile_entry_time,
                               h3_gps.tile_exit_time),

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
       utilisation

FROM capacity_percentages AS cp
INNER JOIN `rj-smtr-dev`.mit_ipea_project.utilisation_observations
ON
    RIGHT(cp.vehicle_id, 5) = utilisation_observations.vehicle_id -- Same Vehicle
    AND cp.as_at = utilisation_observations.date -- Same date
    AND EXTRACT(TIME FROM datetime)
        BETWEEN TIME_SUB(tile_entry_time, INTERVAL 0 MINUTE)
        AND TIME_ADD(tile_exit_time, INTERVAL 0 MINUTE) -- Obs time match

LEFT JOIN `rj-smtr-dev`.mit_ipea_project.vw_h3
    ON cp.tile_id = vw_h3.tile_id

ORDER BY  int64_field_0


