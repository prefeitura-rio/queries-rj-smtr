{{
  config(
    alias='operadora_transporte',
  )
}}

WITH 
    operadora_transporte AS (
        SELECT
            data,
            SAFE_CAST(CD_OPERADORA_TRANSPORTE, AS STRING) AS cd_operadora_transporte,
            timestamp_captura,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.CD_CLIENTE') AS STRING) AS CD_CLIENTE,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_CLIENTE') AS STRING) AS CD_TIPO_CLIENTE,
            SAFE_CAST(CD_TIPO_MODAL, AS STRING) AS CD_TIPO_MODAL,
            SAFE_CAST(IN_SITUACAO_ATIVIDADE, AS STRING) AS IN_SITUACAO_ATIVIDADE
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "operadora_transporte") }}
    ),
    operadora_transporte_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_operadora_transporte) AS rn
        FROM
            operadora_transporte
    )
SELECT
  * EXCEPT(rn)
FROM
  operadora_transporte_rn
WHERE
  rn = 1