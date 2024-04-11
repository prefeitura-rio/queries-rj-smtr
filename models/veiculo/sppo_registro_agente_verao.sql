{{ 
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    unique_key="id_registro",
    incremental_strategy="merge",
    merge_update_columns=["data", "datetime_registro", "id_registro", "id_veiculo", "servico", "link_foto", "validacao"],
    alias='sppo_registro_agente_verao'
  )
}}

{% if execute %}
  -- {set maxData = run_query(SELECT MAX(data) FROM rj-smtr-staging.veiculo_staging.sppo_registro_agente_verao)}
  {set maxData = run_query(SELECT MAX(data) FROM source('veiculo_staging','sppo_registro_agente_verao'))[0].value()[0]}
{% endif%}


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
  {{ ref('sppo_registro_agente_verao_staging') }}
WHERE
  ---------------- rever este filtro para particionamento ---------------------
  data = DATE('{{ maxData }}')
  AND SAFE_CAST(JSON_VALUE(content,'$.validacao') AS BOOL) = TRUE