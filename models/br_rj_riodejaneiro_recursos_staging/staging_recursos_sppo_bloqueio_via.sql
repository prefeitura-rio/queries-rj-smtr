{{ config(
  materialized = 'view',
  alias='recursos_sppo_bloqueio_via',
  )
}}



SELECT
    ROW_NUMBER() OVER(PARTITION BY protocol ORDER BY timestamp_captura DESC) AS rn,
    JSON_EXTRACT_ARRAY(content, '$.customFieldValues') AS items,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', REGEXP_REPLACE(JSON_VALUE(content, '$.createdDate'), r'(\.\d+)?$', '')), 'America/Sao_Paulo') AS datetime_recurso,
    SAFE_CAST(protocol AS STRING) AS id_recurso,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', REGEXP_REPLACE(JSON_VALUE(content, '$.lastUpdate'), r'(\.\d+)?$', '')), 'America/Sao_Paulo') AS datetime_update,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), 'America/Sao_Paulo') AS datetime_captura,
    data 
  FROM 
    {{source('br_rj_riodejaneiro_recursos_staging', 
      'recursos_sspo_bloqueio_via')}}
