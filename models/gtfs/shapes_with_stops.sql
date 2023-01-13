
-- 4. remove duplicates, keep with min distance
SELECT *
    EXCEPT (
        -- Remove extra columns
        min_distance,
        distance
    )
FROM
(
    -- 3. set min distance
    SELECT *,
        MIN(distance) OVER (PARTITION BY stop_id) AS min_distance
    FROM
    (
        -- 2. shapes_with_stops with distance
        SELECT
            SAFE_CAST(stoptimes.trip_id AS STRING) as trip_id,
            SAFE_CAST(stoptimes.stop_id AS STRING) as stop_id,
            SAFE_CAST(stops.stop_name AS STRING) as stop_name,
            SAFE_CAST(shapes.shape_pt_lat AS FLOAT64) AS latitude,
            SAFE_CAST(shapes.shape_pt_lon AS FLOAT64) AS longitude,
            SAFE_CAST(shapes.shape_dist_traveled AS INT) as shape_dist_traveled,
            SAFE_CAST(stoptimes.stop_sequence AS INT64) as stop_sequence,
            SAFE_CAST(routes.route_id AS INT) as route_id,
            SAFE_CAST(routes.route_short_name AS INT) as route_short_name,
            SAFE_CAST(routes.route_long_name AS INT) as route_long_name,
            SAFE_CAST(previous_stop_id AS STRING) as previous_stop_id,
            SAFE_CAST(previous_stop_name AS STRING) as previous_stop_name,
            SAFE_CAST(next_stop_id AS STRING) as next_stop_id,
            SAFE_CAST(next_stop_name AS STRING) as next_stop_name,

            -- extra cols, for debug
            -- SAFE_CAST(DATETIME(TIMESTAMP(stoptimes.timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
            -- SAFE_CAST(shapes.shape_id AS STRING) AS shape_id,
            -- SAFE_CAST(shapes.shape_pt_lat AS FLOAT64) AS shape_pt_lat,
            -- SAFE_CAST(shapes.shape_pt_lon AS FLOAT64) AS shape_pt_lon,
            -- SAFE_CAST(stops.stop_lat AS FLOAT64) AS stop_lat,
            -- SAFE_CAST(stops.stop_lon AS FLOAT64) AS stop_lon,
            -- SAFE_CAST(previous_stop_sequence AS INT64) AS previous_stop_sequence,
            -- SAFE_CAST(next_stop_sequence AS INT64) AS next_stop_sequence,
            ST_DISTANCE(
                ST_GEOGPOINT(SAFE_CAST(shape_pt_lon AS FLOAT64), SAFE_CAST(shape_pt_lat AS FLOAT64)),
                ST_GEOGPOINT(SAFE_CAST(stop_lon AS FLOAT64), SAFE_CAST(stop_lat AS FLOAT64))
            ) AS distance
        FROM
        (
            -- 1. stoptimes with extra cols
            SELECT *,
                (SELECT stop_name FROM {{ var('stops_staging') }} WHERE stop_id = previous_stop_id) AS previous_stop_name,
                (SELECT stop_name FROM {{ var('stops_staging') }} WHERE stop_id = next_stop_id) AS next_stop_name,
            FROM (
                SELECT stoptimes_1.*,
                    stops.stop_name,
                    LAG(SAFE_CAST(stoptimes_1.stop_sequence AS INT64)) OVER (PARTITION BY SAFE_CAST(stoptimes_1.trip_id AS STRING) ORDER BY SAFE_CAST(stoptimes_1.stop_sequence AS INT64)) AS previous_stop_sequence,
                    LEAD(SAFE_CAST(stoptimes_1.stop_sequence AS INT64)) OVER (PARTITION BY SAFE_CAST(stoptimes_1.trip_id AS STRING) ORDER BY SAFE_CAST(stoptimes_1.stop_sequence AS INT64)) AS next_stop_sequence,
                    LAG(SAFE_CAST(stoptimes_1.stop_id AS STRING)) OVER (PARTITION BY SAFE_CAST(stoptimes_1.trip_id AS STRING) ORDER BY SAFE_CAST(stoptimes_1.stop_sequence AS INT64)) AS previous_stop_id,
                    LEAD(SAFE_CAST(stoptimes_1.stop_id AS STRING)) OVER (PARTITION BY SAFE_CAST(stoptimes_1.trip_id AS STRING) ORDER BY SAFE_CAST(stoptimes_1.stop_sequence AS INT64)) AS next_stop_id,
                FROM {{ var('stop_times_staging') }} stoptimes_1
                JOIN {{ var('stops_staging') }} stops ON (stops.timestamp_captura BETWEEN '2022-12-27 10:00:00-03:00' AND '2022-12-27 11:00:00-03:00'  AND stops.stop_id = stoptimes_1.stop_id)
                WHERE stoptimes_1.timestamp_captura BETWEEN '2022-12-27 10:00:00-03:00' AND '2022-12-27 11:00:00-03:00' 
            )
            ORDER BY trip_id, SAFE_CAST(stop_sequence AS INT64)
            -- 1
        ) stoptimes
            JOIN {{ var('trips_staging') }} trips ON (SAFE_CAST(trips.trip_id AS STRING) = stoptimes.trip_id AND stoptimes.timestamp_captura BETWEEN '2022-12-27 10:00:00-03:00' AND '2022-12-27 11:00:00-03:00' )
            JOIN {{ var('stops_staging') }} stops ON (SAFE_CAST(stoptimes.stop_id AS STRING) = stops.stop_id AND stops.timestamp_captura BETWEEN '2022-12-27 10:00:00-03:00' AND '2022-12-27 11:00:00-03:00' )
            JOIN {{ var('shapes_staging') }} shapes ON (SAFE_CAST(trips.shape_id AS STRING) = shapes.shape_id AND shapes.timestamp_captura BETWEEN '2022-12-27 10:00:00-03:00' AND '2022-12-27 11:00:00-03:00' )
            JOIN {{ var('routes_staging') }} routes ON (SAFE_CAST(trips.route_id AS STRING) = routes.route_id AND routes.timestamp_captura BETWEEN '2022-12-27 10:00:00-03:00' AND '2022-12-27 11:00:00-03:00' )
        -- 2
    )
    ORDER BY stop_id, stop_sequence, trip_id
    -- 3
)
WHERE distance = min_distance
LIMIT 2
-- 4
