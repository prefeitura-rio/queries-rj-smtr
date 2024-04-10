{{ 
  config(
    partition_by = { 
      "field": "feed_start_date",
      "data_type": "date",
      "granularity": "day"
    },
    alias = "ordem_servico_trajeto_alternativo",
  ) 
}} 

WITH 
  ordem_servico_trajeto_alternativo AS (
    SELECT 
      fi.feed_version,
      SAFE_CAST(o.data_versao AS DATE) feed_start_date,
      fi.feed_end_date,
      SAFE_CAST(o.servico AS STRING) servico,
      SAFE_CAST(JSON_VALUE(o.content, "$.ativacao") AS STRING) ativacao,
      SAFE_CAST(JSON_VALUE(o.content, "$.consorcio") AS STRING) consorcio,
      SAFE_CAST(JSON_VALUE(o.content, "$.descricao") AS STRING) descricao,
      SAFE_CAST(JSON_VALUE(o.content, "$.evento") AS STRING) evento,
      SAFE_CAST(JSON_VALUE(o.content, "$.extensao_ida") AS FLOAT64)/1000 extensao_ida,
      SAFE_CAST(JSON_VALUE(o.content, "$.extensao_volta") AS FLOAT64)/1000 extensao_volta,
      SAFE_CAST(JSON_VALUE(o.content, "$.horario_inicio") AS STRING) horario_inicio,
      SAFE_CAST(JSON_VALUE(o.content, "$.horario_fim") AS STRING) horario_fim,
      SAFE_CAST(JSON_VALUE(o.content, "$.vista") AS STRING) vista,
    FROM 
      {{ source("br_rj_riodejaneiro_gtfs_staging", "ordem_servico_trajeto_alternativo") }} O
    LEFT JOIN 
      {{ ref("feed_info_gtfs2") }} fi 
    ON 
      o.data_versao = CAST(fi.feed_start_date AS STRING)
    {% if is_incremental() -%}
      WHERE 
        o.data_versao = "{{ var('data_versao_gtfs') }}"
        AND fi.feed_start_date = "{{ var('data_versao_gtfs') }}"
    {%- endif %}
  ),
  ordem_servico_trajeto_alternativo_sentido AS (
    SELECT
      *
    FROM
      ordem_servico_trajeto_alternativo
    UNPIVOT 
    (
      (
        distancia_planejada
      ) FOR sentido IN (
        (
          extensao_ida
        ) AS "I",
        (
          extensao_volta
        ) AS "V"
      )
    )
  )
SELECT
  feed_version,
  feed_start_date,
  feed_end_date,
  servico,
  consorcio,
  vista,
  ativacao,
  descricao,
  evento,
  distancia_planejada,
  CASE
    WHEN "C" IN UNNEST(sentido_array) THEN "C"
    ELSE o.sentido
  END AS sentido,
  horario_inicio,
  horario_fim,
  "{{ var('version') }}" AS versao_modelo
FROM
  ordem_servico_trajeto_alternativo_sentido AS o
LEFT JOIN
  (
    SELECT
      feed_start_date,
      servico,
      ARRAY_AGG(sentido) AS sentido_array,
    FROM
      {{ ref("ordem_servico_gtfs2") }}
    GROUP BY
      1,
      2
    {% if is_incremental() -%}
      WHERE 
        feed_start_date = "{{ var('data_versao_gtfs') }}"
    {%- endif %}
  ) AS s
USING
  (feed_start_date, servico)
WHERE
  distancia_planejada != 0