{{
  config(
    alias='gratuidade',
  )
}}

WITH gratuidade AS (
  SELECT
    data,
        SAFE_CAST(id AS STRING) AS id,
        timestamp_captura,
        SAFE_CAST(JSON_VALUE(content, '$.cd_cliente') AS STRING) AS cd_cliente,
        DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_inclusao') AS STRING)), 'America/Sao_Paulo') AS data_inclusao,
        SAFE_CAST(JSON_VALUE(content, '$.id_status_gratuidade') AS STRING) AS id_status_gratuidade,
        SAFE_CAST(JSON_VALUE(content, '$.id_tipo_gratuidade') AS STRING) AS id_tipo_gratuidade
  FROM
    {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'gratuidade') }}
)
SELECT 
  * EXCEPT(rn)
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
  FROM
    gratuidade
)
WHERE
  rn = 1
