-- Purpose: Create a view that assigns each GPS ping to a H3 tile

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

-- Union All, both BRT and SPPO gps tables
WITH brt_sppo_gps AS (

    SELECT 
        modo, timestamp_gps, data, hora, id_veiculo, servico, latitude, longitude,
        flag_em_operacao, flag_em_movimento, tipo_parada, flag_linha_existe_sigmob,
        flag_trajeto_correto, flag_trajeto_correto_hist, status, velocidade_instantanea,
        velocidade_estimada_10_min, distancia, versao
    FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    WHERE data = "{{ var('run_date') }}" -- This is a hardcoded date to allow for testing. This should be removed in production.

    UNION ALL

    SELECT 
        modo, timestamp_gps, data, hora, id_veiculo, servico, latitude, longitude,
        flag_em_operacao, flag_em_movimento, tipo_parada, flag_linha_existe_sigmob,
        flag_trajeto_correto, flag_trajeto_correto_hist, status, velocidade_instantanea,
        velocidade_estimada_10_min, distancia, versao
    FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_brt`
    WHERE data = "{{ var('run_date') }}" -- This is a hardcoded date to allow for testing. This should be removed in production.
), 

-- Rename columns into english
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
        flag_em_movimento AS in_motion,
        status,
        velocidade_instantanea AS velocity_instantaneous,
        velocidade_estimada_10_min AS est_ten_min_velocity,
        distancia AS distance,
        versao AS version,
        ST_GEOGPOINT(longitude, latitude) AS geography
    FROM brt_sppo_gps
    
),

-- Assign each GPS ping to a H3 tile
-- Many H3
gps_h3 AS (
    SELECT 
        *,
        CASE
            WHEN LAG(tile_id) OVER (ORDER BY vehicle_id, as_at, time) = tile_id THEN 0
            ELSE 1
        END AS tile_entry -- 1 when the vehicle first enters the tile. If 0, the vehicle is already in the tile.

    FROM gps
    JOIN `rj-smtr-dev.mit_ipea_project.vw_h3` AS h3 -- This appears to be an inner join which isn't ideal (may drop data). Left join doesn't seem to work.
        ON ST_INTERSECTS(gps.geography, h3.geometry)

)

SELECT *,
    time AS tile_entry_time,
    LEAD(time) OVER (PARTITION BY vehicle_id ORDER BY as_at, time) AS tile_exit_time
FROM gps_h3
WHERE tile_entry = 1