{{
  config(
    alias='conta_bancaria',
  )
}}

WITH 
    conta_bancaria AS (
        SELECT
            data,
            SAFE_CAST(CD_CLIENTE AS STRING) AS cd_cliente,
            timestamp_captura,
            SAFE_CAST(JSON_VALUE(content, '$.CD_AGENCIA') AS STRING) AS cd_agencia,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_CONTA') AS STRING) AS cd_tipo_conta,
            SAFE_CAST(JSON_VALUE(content, '$.NM_BANCO') AS STRING) AS nm_banco,
            SAFE_CAST(JSON_VALUE(content, '$.NR_BANCO') AS STRING) AS nr_banco,
            SAFE_CAST(JSON_VALUE(content, '$.NR_CONTA') AS STRING) AS nr_conta,
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "conta_bancaria") }}
    ),
    conta_bancaria_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_cliente ORDER BY timestamp_captura DESC) AS rn
        FROM
            conta_bancaria
    )
SELECT
  * EXCEPT(rn)
FROM
  conta_bancaria_rn
WHERE
  rn = 1