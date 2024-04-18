-- depends_on: {{ ref('ordem_servico_trajeto_alternativo_gtfs2') }}
/*
Identificação de um trip de referência para cada serviço e sentido regular
Identificação de todas as trips de referência para os trajetos alternativos
*/

{{
  config(
    materialized='ephemeral'
  )
}}

{% if execute -%}
  {% if is_incremental() -%}
    {%- set query = "SELECT DISTINCT evento FROM " ~ ref('ordem_servico_trajeto_alternativo_gtfs2') ~ " WHERE feed_start_date = '" ~ var('data_versao_gtfs')  ~ "'" -%}
  {% else %}
    {%- set query = "SELECT DISTINCT evento FROM " ~ ref('ordem_servico_trajeto_alternativo_gtfs2') -%}
  {% endif -%}
  {%- set eventos_trajetos_alternativos = run_query(query).columns[0].values() -%}
{% endif %}

WITH 
  -- 1. Busca os shapes em formato geográfico
  shapes AS (
    SELECT
      *
    FROM
      {{ ref("shapes_geom_gtfs2") }}
    {% if is_incremental() -%}
    WHERE 
      feed_start_date = '{{ var("data_versao_gtfs") }}'
    {% endif -%}
  ),
  -- 2. Busca as trips
  trips_all AS (
    SELECT
      *,
      CASE
        WHEN indicador_trajeto_alternativo = TRUE THEN CONCAT(feed_version, trip_short_name, tipo_dia, direction_id, shape_id)
        ELSE CONCAT(feed_version, trip_short_name, tipo_dia, direction_id)
      END AS trip_partition
    FROM
    (
      SELECT
        service_id,
        trip_id,
        trip_headsign,
        trip_short_name,
        direction_id,
        shape_id,
        feed_version,
        shape_distance,
        start_pt,
        end_pt,
        CASE
          WHEN service_id LIKE "%U_%" THEN "Dia Útil"
          WHEN service_id LIKE "%S_%" THEN "Sabado"
          WHEN service_id LIKE "%D_%" THEN "Domingo"
        ELSE
        service_id
      END
        AS tipo_dia,
        CASE WHEN (
          {% for evento in eventos_trajetos_alternativos %}
          trip_headsign LIKE "%{{evento}}%" OR
          {% endfor %}
          service_id = "EXCEP") THEN TRUE
        ELSE FALSE
      END
        AS indicador_trajeto_alternativo,
      FROM
        {{ ref("trips_gtfs2") }}
      LEFT JOIN
        shapes
      USING
        (feed_start_date,
        feed_version,
        shape_id)
      WHERE
        {% if is_incremental() -%}
        feed_start_date = '{{ var("data_versao_gtfs") }}' AND
        {% endif %}
        service_id NOT LIKE "%_DESAT_%"  -- Desconsidera service_ids desativados
    )
  )
-- 3. Busca as trips de referência para cada serviço, sentido, e tipo_dia
SELECT
    * EXCEPT(rn)
FROM
(
    SELECT
    * EXCEPT(shape_distance),
    ROW_NUMBER() OVER (PARTITION BY trip_partition ORDER BY feed_version, trip_short_name, tipo_dia, direction_id, shape_distance DESC) AS rn
    FROM
    trips_all
)
WHERE
    rn = 1