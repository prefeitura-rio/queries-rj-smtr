{{ config(
  materialized = "incremental",
  partition_by = { "field" :"data_recurso",
    "data_type" :"date",
    "granularity": "day" },
      unique_key = ["protocol", "data_recurso"],
      alias = "recurso_sppo",
  incremental_strategy = 'insert_overwrite',

) }}


WITH recurso_sppo AS (
  SELECT
    DISTINTCT *,
     JSON_EXTRACT_ARRAY(content, '$.customFieldValues') AS items
  FROM
    {{source(
      "br_rj_riodejaneiro_recurso_staging",
      "recurso_sppo"
   )}}
  {% if is_incremental() -%}
  WHERE 
    date(data)  BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  {%- endif %} 
),
exploded AS (
  SELECT
    DISTINCT 
    SAFE_CAST(protocol AS STRING) AS id_recurso,
    SAFE_CAST(timestamp_captura AS DATETIME) AS timestamp_captura,
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', SAFE_CAST(JSON_VALUE(content, '$.createdDate') AS STRING)), "America/Sao_Paulo") AS data_recurso,
    SAFE_CAST(JSON_VALUE(content, "$.id") AS STRING) AS id_ticket,
    SAFE_CAST(COALESCE(JSON_VALUE(items, '$.value'), JSON_VALUE(items, '$.items[0].customFieldItem')) AS STRING) AS value,
    SAFE_CAST(JSON_EXTRACT(items, '$.customFieldId') AS STRING) AS field_id,
  FROM
    recurso_sppo,
    UNNEST(items) items 
),
pivotado AS (
  SELECT
    DISTINCT *
  FROM
    exploded PIVOT(ANY_VALUE(value) FOR field_id IN ('111870',
        '111871',
        '111872',
        '111873',
        '111901',
        '111865',
        '111867',
        '111868',
        '111869',
        '111866',
        '111904',
        '125615',
        '111900')) )
SELECT
  DISTINCT
  id_recurso,
  timestamp_captura,
  data_recurso,
  id_ticket,
  SAFE_CAST(p.111865 AS STRING) AS julgamento,
  SAFE_CAST(p.111870 AS STRING) AS consorcio,
  SAFE_CAST(p.111871 AS STRING) AS linha,
  SAFE_CAST(p.111872 AS STRING) AS tipo_servico,
  SAFE_CAST(p.111873 AS STRING) AS id_veiculo,
  SAFE_CAST(p.111901 AS STRING) AS sentido,
  PARSE_DATE('%Y%m%d', SAFE_CAST(p.111867 AS STRING)) AS data_viagem, # date
  PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', SAFE_CAST(p.111868 AS STRING), 'America/Sao_Paulo') AS hora_inicio_viagem,
  PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', SAFE_CAST(p.111869 AS STRING), 'America/Sao_Paulo') hora_fim_viagem, # datetime
  SAFE_CAST(p.111866 AS STRING) AS motivo,
  SAFE_CAST(p.111904 AS STRING) AS motivo_indeferido,
  SAFE_CAST(p.111900 AS STRING) AS motivo_deferido,
  SAFE_CAST(p.125615 AS STRING) AS observacao
FROM
  pivotado p

