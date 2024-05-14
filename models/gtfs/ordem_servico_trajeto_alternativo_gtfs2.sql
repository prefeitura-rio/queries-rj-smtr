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

-- TBD: Alterar tabela em staging ou manter o tratamento assim?
WITH
  ordem_servico_trajeto_alternativo_array AS (
    SELECT
      data_versao,
      servico,
      ARRAY(
        SELECT
          CONCAT('{', TRIM(item, '{}'), '}')
        FROM
          UNNEST(SPLIT(content, "},{")) AS item
      ) AS content
    FROM
      {{ source("br_rj_riodejaneiro_gtfs_staging", "ordem_servico_trajeto_alternativo") }}
    {% if is_incremental() -%}
      WHERE 
        data_versao = '{{ var("data_versao_gtfs") }}'
    {%- endif %}
  ),
  ordem_servico_trajeto_alternativo AS (
  SELECT 
    fi.feed_version,
    SAFE_CAST(data_versao AS DATE) feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(servico AS STRING) servico,
    SAFE_CAST(JSON_VALUE(content, "$.ativacao") AS STRING) ativacao,
    SAFE_CAST(JSON_VALUE(content, "$.consorcio") AS STRING) consorcio,
    SAFE_CAST(JSON_VALUE(content, "$.descricao") AS STRING) descricao,
    SAFE_CAST(JSON_VALUE(content, "$.evento") AS STRING) evento,
    SAFE_CAST(JSON_VALUE(content, "$.extensao_ida") AS FLOAT64) extensao_ida,
    SAFE_CAST(JSON_VALUE(content, "$.extensao_volta") AS FLOAT64) extensao_volta,
    SAFE_CAST(JSON_VALUE(content, "$.horario_inicio") AS STRING) horario_inicio,
    SAFE_CAST(JSON_VALUE(content, "$.horario_fim") AS STRING) horario_fim,
    SAFE_CAST(JSON_VALUE(content, "$.vista") AS STRING) vista,
    COALESCE(SAFE_CAST(JSON_VALUE(content, '$.tipo_os') AS STRING), "Regular") tipo_os,
  FROM 
    ordem_servico_trajeto_alternativo_array,
    UNNEST(content) AS content
  LEFT JOIN 
    {{ ref("feed_info_gtfs2") }} fi 
  ON 
    data_versao = CAST(fi.feed_start_date AS STRING)
  {% if is_incremental() -%}
    WHERE 
      data_versao = "{{ var('data_versao_gtfs') }}"
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
  horario_inicio AS inicio_periodo,
  horario_fim AS fim_periodo,
  tipo_os,
  '{{ var("version") }}' AS versao_modelo
FROM
  ordem_servico_trajeto_alternativo