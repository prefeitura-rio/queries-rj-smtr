{{
  config(
    alias='linha_sem_ressarcimento',
  )
}}

WITH linha_sem_ressarcimento AS (
  SELECT
    data,
    SAFE_CAST(id_linha AS STRING) AS id_linha,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.dt_inclusao') AS STRING)), 'America/Sao_Paulo') AS dt_inclusao
  FROM
    {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'linha_sem_ressarcimento') }}
)
SELECT 
  * EXCEPT(rn)
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_linha ORDER BY timestamp_captura DESC) AS rn
  FROM
    linha_sem_ressarcimento
)
WHERE
  rn = 1
