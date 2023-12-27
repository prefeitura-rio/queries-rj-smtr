{{
  config(
    alias='percentual_rateio_integracao',
  )
}}

WITH percentual_rateio_integracao AS (
  SELECT
    data,
    SAFE_CAST(id AS STRING) AS id,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
    SAFE_CAST(JSON_VALUE(content, '$.dt_fim_validade') AS STRING) AS dt_fim_validade,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:S%Ez', SAFE_CAST(JSON_VALUE(content, '$.dt_inclusao') AS STRING)), 'America/Sao_Paulo') AS dt_inclusao,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d', SAFE_CAST(JSON_VALUE(content, '$.dt_inicio_validade') AS STRING)), 'America/Sao_Paulo') AS dt_inicio_validade,
    REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_integracao_t1') AS STRING), '.0', '') AS id_tipo_modal_integracao_t1,
    REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_integracao_t2') AS STRING), '.0', '') AS id_tipo_modal_integracao_t2,
    REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_integracao_t3') AS STRING), '.0', '') AS id_tipo_modal_integracao_t3,
    REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_integracao_t4') AS STRING), '.0', '') AS id_tipo_modal_integracao_t4,
    REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_origem') AS STRING), '.0', '') AS id_tipo_modal_origem,
    SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_integracao_t1') AS FLOAT64) AS perc_rateio_integracao_t1,
    SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_integracao_t2') AS FLOAT64) AS perc_rateio_integracao_t2,
    SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_integracao_t3') AS FLOAT64) AS perc_rateio_integracao_t3,
    SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_integracao_t4') AS FLOAT64) AS perc_rateio_integracao_t4,
    SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_origem') AS FLOAT64) AS perc_rateio_origem
  FROM
    {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'percentual_rateio_integracao') }}
)
SELECT 
  * EXCEPT(rn)
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
  FROM
    percentual_rateio_integracao
)
WHERE
  rn = 1