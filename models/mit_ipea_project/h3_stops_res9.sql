{{ config(materialized='table') }}

-- Purpose: Create a table that assigns each GPS ping to a H3 tile
-- Rename columns into english, join on H3 table via a circle fully encapsulating each tile
-- Each row is now a set of possible H3 tiles for each observation
WITH gps AS (
    SELECT
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        ST_GEOGPOINT(stop_lon, stop_lat) AS geography,
        tile_id
    FROM `rj-smtr`.gtfs.stops
    LEFT JOIN {{ ref("vw_h3_res9") }} AS h3
    --LEFT JOIN `rj-smtr-dev`.mit_ipea_project.vw_h3 AS h3
        ON ST_DWITHIN(ST_GEOGPOINT(stop_lon, stop_lat), h3.centroid, 560)

    WHERE feed_version = (SELECT MAX(feed_version) FROM `rj-smtr`.gtfs.stops)
),

h3_gps AS (
    SELECT
        *
    FROM gps
    LEFT JOIN {{ ref("vw_h3_res9") }} AS h3
    --LEFT JOIN `rj-smtr-dev`.mit_ipea_project.vw_h3 AS h3
        USING (tile_id)

    WHERE ST_INTERSECTS(gps.geography, h3.geometry) IS TRUE

)
SELECT
    *
FROM h3_gps