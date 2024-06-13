{{ config(
  materialized = 'incremental',
  partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
      unique_key = 'id_recurso',
) }}

WITH exploded AS (
  SELECT
    id_recurso,
    datetime_recurso,
    datetime_captura,
    datetime_update, 
    SAFE_CAST(COALESCE(JSON_VALUE(items, '$.value'), JSON_VALUE(items, '$.items[0].customFieldItem')) AS STRING
    ) AS value, 
    SAFE_CAST(JSON_EXTRACT(items, '$.customFieldId') AS STRING ) AS field_id 
  FROM 
    {{ ref('staging_recursos_sppo_reprocessamento') }}, 
    UNNEST(items) items

  {% if is_incremental() -%}
  WHERE
    DATE(data) BETWEEN DATE("{{var('date_range_start')}}") 
        AND DATE("{{var('date_range_end')}}")
  {%- endif %}
 
), 
pivotado AS (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY id_recurso ORDER BY datetime_captura DESC) AS rn,
  FROM 
    exploded PIVOT(
      ANY_VALUE(value) FOR field_id IN (
        '113816', '113817', '111865','111866','111904',
        '111900', '125615'
      )
    )
), 
tratado AS (
  SELECT 
    id_recurso, 
    datetime_captura, 
    datetime_recurso,
    datetime_update,
    SAFE_CAST(p.111865 AS STRING) AS julgamento, 
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', SAFE_CAST(p.113816 AS STRING), 'America/Sao_Paulo') AS data_hora_inicio, 
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', SAFE_CAST(p.113817 AS STRING), 'America/Sao_Paulo') AS data_hora_fim, 
    COALESCE(SAFE_CAST(p.111904 AS STRING), SAFE_CAST(p.111900 AS STRING)) AS motivo_julgamento, 
    SAFE_CAST(p.125615 AS STRING) AS observacao,
   
  FROM 
    pivotado p
  WHERE rn = 1
) 
SELECT
      t.id_recurso,
      DATE(datetime_recurso) AS data,
      t.datetime_captura,
      t.datetime_recurso,
      t.datetime_update,
      DATETIME(TIMESTAMP_SUB(data_hora_inicio, INTERVAL 3 HOUR)) AS data_hora_inicio_viagem,
      DATETIME(TIMESTAMP_SUB(data_hora_fim, INTERVAL 3 HOUR)) AS data_hora_fim_viagem,
      t.julgamento,
      t.motivo_julgamento,
      t.observacao AS observacao_julgamento,
      j.data_julgamento
  
FROM
      tratado t
      
LEFT JOIN 

    {{ ref('recursos_sppo_reprocessamento_ultimo_julgamento') }} AS j
    
  ON t.id_recurso = j.id_recurso

