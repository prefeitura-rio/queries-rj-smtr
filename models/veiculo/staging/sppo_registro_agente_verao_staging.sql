{{ config(
        materialized="view",
        alias='sppo_registro_agente_verao'
)
}}

SELECT
  SAFE_CAST(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", datetime_registro) AS DATE) AS data,
  SAFE_CAST(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", datetime_registro) AS DATETIME) AS datetime_registro,
  SHA256(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", datetime_registro) || "_" || SAFE_CAST(email AS STRING)) AS id_registro,
  SAFE_CAST(JSON_VALUE(content,'$.id_veiculo') AS STRING) AS id_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.servico') AS STRING) AS servico,
  SAFE_CAST(JSON_VALUE(content,'$.link_foto') AS STRING) AS link_foto,
  SAFE_CAST(JSON_VALUE(content,'$.validacao') AS BOOL) AS validacao,
  SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) AS datetime_captura,
  "{{ var("version") }}" AS versao
FROM
  {{ source('veiculo_staging','sppo_registro_agente_verao') }}
