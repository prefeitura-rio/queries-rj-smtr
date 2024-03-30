{{
    config(
        materialized="view",
    )
}}

SELECT
  SAFE_CAST(status AS STRING) status,
  SAFE_CAST(subsidio_km AS FLOAT64) subsidio_km,
  SAFE_CAST(irk AS FLOAT64) irk,
  SAFE_CAST(data_inicio AS DATE) data_inicio,
  SAFE_CAST(data_fim AS DATE) data_fim,
  SAFE_CAST(legislacao AS STRING) legislacao
FROM
  {{ source("dashboard_subsidio_sppo_staging", "subsidio_valor_km_tipo_viagem") }}