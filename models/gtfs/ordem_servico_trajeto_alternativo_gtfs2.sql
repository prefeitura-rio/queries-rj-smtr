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

WITH ordem_servico_trajeto_alternativo AS (
  SELECT 
    fi.feed_version,
    SAFE_CAST(o.data_versao AS DATE) feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(o.servico AS STRING) servico,
    SAFE_CAST(JSON_VALUE(o.content, "$.ativacao") AS STRING) ativacao,
    SAFE_CAST(JSON_VALUE(o.content, "$.consorcio") AS STRING) consorcio,
    SAFE_CAST(JSON_VALUE(o.content, "$.descricao") AS STRING) descricao,
    SAFE_CAST(JSON_VALUE(o.content, "$.evento") AS STRING) evento,
    SAFE_CAST(JSON_VALUE(o.content, "$.extensao_ida") AS FLOAT64) extensao_ida,
    SAFE_CAST(JSON_VALUE(o.content, "$.extensao_volta") AS FLOAT64) extensao_volta,
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
  CASE
    WHEN evento LIKE '[%]' THEN LOWER(evento)
    ELSE REGEXP_REPLACE(LOWER(evento), r"([a-záéíóúñüç]+)", r"[\1]")
  END AS evento,
  extensao_ida/1000 AS extensao_ida,
  extensao_volta/1000 AS extensao_volta,
  IF(horario_inicio IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_inicio, ":")) = 3, 
      PARSE_TIME("%T", 
                  CONCAT(
                      SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) AS INT64), 
                      ":", 
                      SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(1)] AS INT64), 
                      ":", 
                      SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(2)] AS INT64)
                  )
                ), 
                NULL
  ) AS inicio_periodo,
  IF(horario_fim IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_fim, ":")) = 3, 
      PARSE_TIME("%T", 
                  CONCAT(
                      SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) AS INT64), 
                      ":", 
                      SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(1)] AS INT64), 
                      ":", 
                      SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(2)] AS INT64)
                  )
                ), 
                NULL
  ) AS fim_periodo,
  '{{ var("version") }}' AS versao_modelo
FROM
  ordem_servico_trajeto_alternativo