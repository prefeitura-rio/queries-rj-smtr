{{ config(
    materialized='incremental',
        partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    },
    unique_key=['data', 'trip_id'],
    incremental_strategy='insert_overwrite'
)
}}

-- => km e viagens planejadas para cada versão do gtfs, trip e service_id (tipo_dia)

-- 1. Calcula o km do dia util => juntar aux_viagem_planejada_dia com shapes
WITH viagens_prod_km_du AS (
  SELECT
    vd.*,
    g.* EXCEPT(shape_id, data_versao, shape_distance),
    CASE
      WHEN ST_DWithin(g.start_pt, g.end_pt, 50) THEN 'C'
      WHEN vd.direction_id = 0 THEN 'I'
      ELSE 'V'
    END AS sentido,
    g.shape_distance AS distancia_planejada,
    vd.viagens_planejadas*shape_distance AS distancia_total_planejada
  FROM {{ ref('aux_viagem_planejada_dia') }} AS vd
  LEFT JOIN `rj-smtr-dev.gtfs_test.shapes_geom` AS g
  ON
    vd.shape_id = g.shape_id
  AND
    vd.data_versao = g.data_versao
),
-- 2. Inclui service_id (tipo_dia) e sentido
viagens_service_id AS (
  SELECT
    pkm_du.*,
    service_id
  FROM viagens_prod_km_du AS pkm_du
  LEFT JOIN UNNEST(['U', 'S', 'D']) AS service_id
)--,
-- 3. Calcula o km de sabado / domingo
--viagens_prod_km AS (
  SELECT
    * EXCEPT(distancia_total_planejada, viagens_planejadas),
    -- CASE
    --   WHEN service_id = 'S' THEN viagens_planejadas*(5/8)
    --   WHEN service_id = 'D' THEN viagens_planejadas*(4/8)
    --   ELSE viagens_planejadas
    -- END AS viagens_planejadas,
    CASE
      WHEN service_id = 'S' THEN distancia_total_planejada*(5/8)
      WHEN service_id = 'D' THEN distancia_total_planejada*(4/8)
      ELSE distancia_total_planejada
    END AS distancia_total_planejada
  FROM viagens_service_id
--)
-- 4. Pega demais informações sobre trips
-- SELECT
--   pkm.* EXCEPT(direction_id),
--   r.route_long_name AS vista,
--   a.agency_name AS consorcio,
--   CASE
--     WHEN ST_DWithin(pkm.start_pt, pkm.end_pt, 50) THEN 'C'
--     WHEN pkm.direction_id = 0 THEN 'I'
--     ELSE 'V'
--   END AS sentido,
-- FROM viagens_prod_km AS pkm
-- LEFT JOIN `rj-smtr-dev.gtfs_test.routes` AS r
-- ON
--   pkm.data_versao = r.data_versao
-- AND
--   pkm.route_id = r.route_id
-- LEFT JOIN `rj-smtr-dev.gtfs_test.agency` AS a
-- ON
--   r.data_versao = a.data_versao
-- AND
--   r.agency_id = a.agency_id