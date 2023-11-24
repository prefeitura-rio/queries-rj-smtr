{{ config(
  materialized="view"
) }} 

WITH
  -- TODO: (1) Generalizar para qualquer tipo_dia
  servicos_exclusivos_sabado AS (
  SELECT
    DISTINCT servico
  FROM
    {{ ref("ordem_servico_gtfs") }}
  WHERE
    -- TODO: (2) Aprimorar critério
    tipo_dia = "Dia Útil"
    AND viagens_planejadas = 0),
  servicos AS (
  SELECT
    -- TODO: (3) Otimizar para apenas as colunas necessárias
    * EXCEPT(versao_modelo,
      shape)
  FROM
    {{ ref("trips_gtfs") }} AS t
  LEFT JOIN
    {{ ref("shapes_geom_gtfs") }} AS s
  USING
    (data_versao,
      shape_id)
  WHERE
    -- TODO: Considerar (1)
    (( trip_short_name NOT IN (SELECT * FROM servicos_exclusivos_sabado)
      AND (service_id LIKE "U%" AND service_id NOT LIKE "U_D%" AND service_id != "U_CP"))
    OR ( trip_short_name IN (SELECT * FROM servicos_exclusivos_sabado)
      AND (service_id LIKE "S%" AND service_id NOT LIKE "S_D%" AND service_id != "S_CP")))
    AND shape_distance IS NOT NULL),
  servicos_rn AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY data_versao, trip_short_name, direction_id ORDER BY trip_short_name, service_id, shape_id, direction_id) AS rn
  FROM
    servicos ),
  servicos_filtrada AS (
  SELECT
    * EXCEPT(rn)
  FROM
    servicos_rn
  WHERE
    rn = 1),
  servicos_potencialmente_circulares AS (
  SELECT
    data_versao,
    trip_short_name,
    COUNT(DISTINCT direction_id) AS q_direcoes
  FROM
    servicos_filtrada
  GROUP BY
    1,
    2
  HAVING
    COUNT(DISTINCT direction_id) = 1 )
SELECT
  data_versao,
  trip_short_name AS servico,
  CASE
    WHEN q_direcoes = 1 AND ST_DISTANCE(start_pt, end_pt) <= 50 THEN "C"
    WHEN direction_id = "0" THEN "I"
    WHEN direction_id = "1" THEN "V"
END
  AS sentido
FROM
  servicos_filtrada AS sf
LEFT JOIN
  servicos_potencialmente_circulares AS spc
USING
  (data_versao,
    trip_short_name)