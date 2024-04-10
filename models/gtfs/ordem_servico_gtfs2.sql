{{ config(
  partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
  unique_key = ['servico', 'feed_start_date'],
  alias = 'ordem_servico'
) }}

WITH 
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
  trips AS (
    SELECT
      * EXCEPT(rn),
    FROM (
      SELECT
        trip_id,
        trip_headsign,
        trip_short_name,
        direction_id,
        shape_id,
        feed_version,
        ROW_NUMBER() OVER (PARTITION BY feed_version, trip_short_name, direction_id ORDER BY feed_version, trip_short_name, direction_id, shape_id) AS rn
      FROM
        {{ ref("trips_gtfs2") }}
      WHERE
        {% if is_incremental() -%}
        feed_start_date = '{{ var("data_versao_gtfs") }}' AND
        {% endif %}
        AND trip_headsign NOT LIKE "%[%" -- Desconsidera trajetos alternativos
        AND service_id NOT LIKE "%_DESAT_%"  -- Desconsidera service_ids desativados
      )
    WHERE
      rn = 1 ),
    servico_trips_sentido AS (
    SELECT
      DISTINCT *
    FROM
      (
        SELECT
          feed_version, 
          trip_short_name AS servico,
          CASE
            WHEN ROUND(ST_Y(start_pt),4) = ROUND(ST_Y(end_pt),4) AND ROUND(ST_X(start_pt),4) = ROUND(ST_X(end_pt),4) THEN "C"
            WHEN direction_id = "0" THEN "I"
            WHEN direction_id = "1" THEN "V"
        END
          AS sentido
        FROM
          trips
        LEFT JOIN
          shapes
        USING
          (feed_version, 
            shape_id)
      )   
      WHERE
        sentido = "C"
    ),
  ordem_servico AS (
    SELECT 
      fi.feed_version,
      SAFE_CAST(os.data_versao AS DATE) as feed_start_date,
      fi.feed_end_date,
      SAFE_CAST(os.servico AS STRING) servico,
      SAFE_CAST(JSON_VALUE(os.content, '$.vista') AS STRING) vista,
      SAFE_CAST(JSON_VALUE(os.content, '$.consorcio') AS STRING) consorcio,
      SAFE_CAST(JSON_VALUE(os.content, '$.horario_inicio') AS STRING) horario_inicio,
      SAFE_CAST(JSON_VALUE(os.content, '$.horario_fim') AS STRING) horario_fim,
      SAFE_CAST(JSON_VALUE(os.content, '$.extensao_ida') AS FLOAT64) extensao_ida,
      SAFE_CAST(JSON_VALUE(os.content, '$.extensao_volta') AS FLOAT64) extensao_volta,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_ida_du') AS FLOAT64) AS INT64) partidas_ida_du,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_volta_du') AS FLOAT64) AS INT64) partidas_volta_du,
      SAFE_CAST(JSON_VALUE(os.content, '$.viagens_du') AS FLOAT64) viagens_du,
      SAFE_CAST(JSON_VALUE(os.content, '$.km_dia_util') AS FLOAT64) km_du,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_ida_pf') AS FLOAT64) AS INT64) partidas_ida_pf,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_volta_pf') AS FLOAT64) AS INT64) partidas_volta_pf,
      SAFE_CAST(JSON_VALUE(os.content, '$.viagens_pf') AS FLOAT64) viagens_pf,
      SAFE_CAST(JSON_VALUE(os.content, '$.km_pf') AS FLOAT64) km_pf,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_ida_sabado') AS FLOAT64) AS INT64) partidas_ida_sabado,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_volta_sabado') AS FLOAT64) AS INT64) partidas_volta_sabado,
      SAFE_CAST(JSON_VALUE(os.content, '$.viagens_sabado') AS FLOAT64) viagens_sabado,
      SAFE_CAST(JSON_VALUE(os.content, '$.km_sabado') AS FLOAT64) km_sabado,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_ida_domingo') AS FLOAT64) AS INT64) partidas_ida_domingo,
      SAFE_CAST(SAFE_CAST(JSON_VALUE(os.content, '$.partidas_volta_domingo') AS FLOAT64) AS INT64) partidas_volta_domingo,
      SAFE_CAST(JSON_VALUE(os.content, '$.viagens_domingo') AS FLOAT64) viagens_domingo,
      SAFE_CAST(JSON_VALUE(os.content, '$.km_domingo') AS FLOAT64) km_domingo,
      -- SAFE_CAST(JSON_VALUE(os.content, '$.tipo_os') AS STRING) tipo_os
      SAFE_CAST(JSON_VALUE(os.content, '$.tipo') AS STRING) tipo_os,
    FROM 
      {{ source(
        'br_rj_riodejaneiro_gtfs_staging',
        'ordem_servico'
      ) }} os
    JOIN
      {{ ref('feed_info_gtfs2') }} fi 
    ON 
      os.data_versao = CAST(fi.feed_start_date AS STRING)
    {% if is_incremental() -%}
      WHERE 
        os.data_versao = '{{ var("data_versao_gtfs") }}'
        AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
    {%- endif %}
  ),
  ordem_servico_tipo_dia AS (
    SELECT 
      *,
      '{{ var("version") }}' as versao_modelo
    FROM ordem_servico UNPIVOT (
        (
          partidas_ida,
          partidas_volta,
          viagens_planejadas,
          distancia_total_planejada
        ) FOR tipo_dia IN (
          (
            partidas_ida_du,
            partidas_volta_du,
            viagens_du,
            km_du
          ) AS 'Dia Útil',
          (
            partidas_ida_pf,
            partidas_volta_pf,
            viagens_pf,
            km_pf
          ) AS 'Ponto Facultativo',
          (
            partidas_ida_sabado,
            partidas_volta_sabado,
            viagens_sabado,
            km_sabado
          ) AS 'Sabado',
          (
            partidas_ida_domingo,
            partidas_volta_domingo,
            viagens_domingo,
            km_domingo
          ) AS 'Domingo'
        )
      )
  ),
  ordem_servico_sentido AS (
    SELECT
      *
    FROM
      ordem_servico_tipo_dia
    UNPIVOT 
    (
      (
        distancia_planejada,
        partidas
      ) FOR sentido IN (
        (
          extensao_ida,
          partidas_ida
        ) AS "I",
        (
          extensao_volta,
          partidas_volta
        ) AS "V"
      )
    )
  )
SELECT 
  o.* EXCEPT(sentido),
  COALESCE(s.sentido, o.sentido) AS sentido
FROM 
  ordem_servico_sentido AS o
LEFT JOIN
  servico_trips_sentido AS s
USING
  (feed_version, servico)
WHERE
  distancia_planejada != 0
  AND distancia_total_planejada != 0