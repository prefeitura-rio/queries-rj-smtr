{{
  config(
    alias="sppo_vistoria_tr_subtt_cglf_2023"
  )
}}

SELECT
  SAFE_CAST(data AS DATE) AS data,
  SAFE_CAST(id_veiculo AS STRING) AS id_veiculo,
  SAFE_CAST(placa AS STRING) AS placa,
  SAFE_CAST(permissao AS STRING) AS permissao,
  SAFE_CAST(chassi AS STRING) AS chassi,
  SAFE_CAST(ano_fabricacao AS INT64) AS ano_fabricacao,
  SAFE_CAST(selo AS STRING) AS selo,
  SAFE_CAST(darm AS STRING) AS darm,
  SAFE_CAST(ano_ultima_vistoria AS INT64) AS ano_ultima_vistoria,
FROM
  {{ source("veiculo_staging", "sppo_vistoria_tr_subtt_cglf_2023") }}