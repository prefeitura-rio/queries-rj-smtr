-- Purpose: Create a table that assigns each GPS ping to a H3 tile

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

WITH
-- Union DISTINCT, both BRT and SPPO gps tables
    -- The tables do contain some duplicates
brt_sppo_gps AS (

    SELECT
        modo, timestamp_gps, data, hora, id_veiculo, servico, latitude, longitude
    FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    WHERE data = "{{ var('run_date') }}" 

    UNION DISTINCT

    SELECT
        modo, timestamp_gps, data, hora, id_veiculo, servico, latitude, longitude
    FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_brt`
    WHERE data = "{{ var('run_date') }}" 
),

-- Rename columns into english, join on H3 table via a circle fully encapsulating each tile
-- Each row is now a set of possible H3 tiles for each observation
gps AS (
    SELECT
        modo AS mode,
        timestamp_gps,
        data AS as_at,
        hora AS time,
        id_veiculo AS vehicle_id,
        servico AS service,
        latitude,
        longitude,
        ST_GEOGPOINT(longitude, latitude) AS geography,
        tile_id
    FROM brt_sppo_gps
    LEFT JOIN {{ ref("vw_h3") }} AS h3
    ON ST_DWITHIN(ST_GEOGPOINT(longitude, latitude), h3.centroid, 560)

),

h3_gps AS (
    SELECT
        *,
        CASE
          WHEN LAG(tile_id) OVER (PARTITION BY vehicle_id ORDER BY timestamp_gps) = tile_id THEN 0
          ELSE 1
        END AS tile_entry -- 1 when the vehicle first enters the tile. If 0, the vehicle is already in the tile.
    FROM gps
    LEFT JOIN {{ ref("vw_h3") }} AS h3
        USING (tile_id)

    WHERE ST_INTERSECTS(gps.geography, h3.geometry) IS TRUE

)
SELECT
    as_at,
    time                                                           AS tile_entry_time,
    LEAD(time) OVER (PARTITION BY vehicle_id ORDER BY as_at, time) AS tile_exit_time,
    mode,
    service,
    vehicle_id,
    longitude,
    latitude,
    tile_id,
    centroid

FROM h3_gps
WHERE tile_entry = 1