{{
  config(
    alias='tipo_modal',
  )
}}

WITH tipo_modal AS (
  SELECT
    data,
    SAFE_CAST(CD_TIPO_MODAL AS STRING) AS cd_tipo_modal,
    timestamp_captura,
    SAFE_CAST(JSON_VALUE(content, '$.DS_TIPO_MODAL') AS STRING) AS ds_tipo_modal
  FROM
    {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'tipo_modal') }}
)
SELECT 
  * EXCEPT(rn)
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY CD_TIPO_MODAL ORDER BY timestamp_captura DESC) AS rn
  FROM
    tipo_modal
)
WHERE
  rn = 1
