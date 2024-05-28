{{
  config(
    alias="sppo_vistoria_tr_subtt_cglf_pendentes_2024"
  )
}}

SELECT
  SAFE_CAST(data AS DATE) AS data,
  SAFE_CAST(id_veiculo AS STRING) AS id_veiculo,
  SAFE_CAST(placa AS STRING) AS placa,
  SAFE_CAST(empresa AS STRING) AS empresa,
  SAFE_CAST(ano_ultima_vistoria AS INT64) AS ano_ultima_vistoria,
FROM
  {{ source("veiculo_staging", "sppo_vistoria_tr_subtt_cglf_pendentes_2024") }}