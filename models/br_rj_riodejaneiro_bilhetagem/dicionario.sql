{{
  config(
    materialized="table",
  )
}}
SELECT
  SAFE_CAST(chave AS STRING) AS chave,
  SAFE_CAST(cobertura_temporal AS STRING) AS cobertura_temporal,
  SAFE_CAST(id_tabela AS STRING) AS id_tabela,
  SAFE_CAST(coluna AS STRING) AS coluna,
  SAFE_CAST(valor AS STRING) AS valor
FROM
  {{ source("br_rj_riodejaneiro_bilhetagem_staging", "dicionario") }}