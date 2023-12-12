{{ config(
  materialized = 'incremental',
  partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
      unique_key = 'id_recurso',
      alias = 'recurso_sppo_viagens_individuais',
) }}


WITH recurso_sppo AS (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY protocol ORDER BY timestamp_captura DESC) AS rn,    
    JSON_EXTRACT_ARRAY(content, '$.customFieldValues') AS items,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', REGEXP_REPLACE(JSON_VALUE(content, '$.createdDate'), r'(\.\d+)?$', '')), 'America/Sao_Paulo') AS datetime_recurso,
    SAFE_CAST(protocol AS STRING) AS id_recurso,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), 'America/Sao_Paulo') AS datetime_captura

  FROM 
    {{source('br_rj_riodejaneiro_recurso_staging', 
      'recurso_sppo_viagens_individuais')}} 
  {% if is_incremental() -%}
    WHERE
        DATE(data) BETWEEN DATE("{{var('date_range_start')}}") 
        AND DATE("{{var('date_range_end')}}")
        AND DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), 
        "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") 
        AND DATETIME("{{var('date_range_end')}}")
  {%- endif %}
), 
exploded AS (
  SELECT 
    id_recurso,
    datetime_recurso,
    datetime_captura, 
    SAFE_CAST(COALESCE(JSON_VALUE(items, '$.value'), JSON_VALUE(items, '$.items[0].customFieldItem')) AS STRING
    ) AS value, 
    SAFE_CAST(JSON_EXTRACT(items, '$.customFieldId') AS STRING ) AS field_id 
  FROM 
    recurso_sppo, 
    UNNEST(items) items
  WHERE
    rn = 1
), 
pivotado AS (
  SELECT *
  FROM 
    exploded PIVOT(
      ANY_VALUE(value) FOR field_id IN (
        '111870', '111871', '111872', '111873', 
        '111901', '111865', '111867', '111868', 
        '111869', '111866', '111904', '125615', 
        '111900'
      )
    )
), 
tratado AS (
  SELECT 
  id_recurso, 
  datetime_captura, 
  datetime_recurso,
  SAFE_CAST(p.111865 AS STRING) AS julgamento, 
  SAFE_CAST(p.111870 AS STRING) AS consorcio,
  CASE
    WHEN SAFE_CAST(p.111872 AS STRING) = "SR - Regular" THEN SAFE_CAST(p.111871 AS STRING)
    ELSE CONCAT(REPLACE(SPLIT(SAFE_CAST(p.111872 AS STRING), "-")[OFFSET(0)], " ", ""), SAFE_CAST(p.111871 AS STRING))
  END AS servico, 
  SAFE_CAST(p.111873 AS STRING) AS id_veiculo, 
  CASE
      WHEN SAFE_CAST(p.111901 AS STRING) = "Ida" THEN "I"
      WHEN SAFE_CAST(p.111901 AS STRING) = "Volta" THEN "V"
      WHEN SAFE_CAST(p.111901 AS STRING) = "Circular" THEN "C"
  END
    AS sentido,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',SAFE_CAST(p.111867 AS STRING), 'America/Sao_Paulo') AS data_viagem, 
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(p.111868 AS STRING), 'America/Sao_Paulo') AS hora_inicio_viagem, 
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(p.111869 AS STRING), 'America/Sao_Paulo') AS hora_fim_viagem, 
  SAFE_CAST(p.111866 AS STRING) AS motivo, 
  COALESCE(SAFE_CAST(p.111904 AS STRING), SAFE_CAST(p.111900 AS STRING)) AS motivo_julgamento, 
  SAFE_CAST(p.125615 AS STRING) AS observacao,
   
FROM 
  pivotado p
)

SELECT
    t.id_recurso,
    t.datetime_captura,
    t.datetime_recurso,
    t.julgamento,
    t.consorcio,
    t.servico,
    t.id_veiculo,
    t.sentido,
    DATETIME(EXTRACT(date FROM TIMESTAMP(data_viagem)), EXTRACT(time FROM TIMESTAMP_SUB(hora_inicio_viagem, INTERVAL 2 HOUR)) ) AS datetime_partida,
    CASE 
      WHEN 
        EXTRACT(time FROM TIMESTAMP_SUB(hora_inicio_viagem, INTERVAL 2 HOUR)) > EXTRACT(time FROM TIMESTAMP_SUB(hora_fim_viagem, INTERVAL 2 HOUR)) 
        
      THEN 
        DATETIME(EXTRACT(date FROM TIMESTAMP_ADD(data_viagem, INTERVAL 1 DAY)), EXTRACT(time FROM TIMESTAMP_SUB(hora_fim_viagem, INTERVAL 2 HOUR))) 
      ELSE 
        DATETIME(EXTRACT(date FROM TIMESTAMP(data_viagem)), 
          EXTRACT(time FROM TIMESTAMP_SUB(hora_fim_viagem, INTERVAL 2 HOUR))
        )
    END AS datetime_chegada,
    t.motivo,
    t.motivo_julgamento,
    t.observacao,
    DATE(datetime_recurso) AS data,
  
FROM
    tratado t
