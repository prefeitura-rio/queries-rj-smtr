
{{ config(
       materialized='view',
       alias='sppo_licenciamento_stu'
)
}}

SELECT
   SAFE_CAST(data AS DATE) data,
   SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) timestamp_captura,
   SAFE_CAST(JSON_VALUE(content,"$.modo") AS STRING) modo,
   SAFE_CAST(id_veiculo AS STRING) id_veiculo,
   SAFE_CAST(JSON_VALUE(content,"$.ano_fabricacao") AS INT64) ano_fabricacao,
   SAFE_CAST(JSON_VALUE(content,"$.carroceria") AS STRING) carroceria,
   CASE
    WHEN JSON_VALUE(content,"$.data_ultima_vistoria") = "" THEN NULL
   ELSE
    SAFE_CAST(PARSE_DATETIME("%d/%m/%Y", JSON_VALUE(content,"$.data_ultima_vistoria")) AS DATE)
   END
   AS data_ultima_vistoria,
   SAFE_CAST(JSON_VALUE(content,"$.id_carroceria") AS INT64) id_carroceria,
   SAFE_CAST(JSON_VALUE(content,"$.id_chassi") AS INT64) id_chassi,
   SAFE_CAST(JSON_VALUE(content,"$.id_fabricante_chassi") AS INT64) id_fabricante_chassi,
   SAFE_CAST(JSON_VALUE(content,"$.id_interno_carroceria") AS INT64) id_interno_carroceria,
   SAFE_CAST(JSON_VALUE(content,"$.id_planta") AS INT64) id_planta,
   SAFE_CAST(JSON_VALUE(content,"$.indicador_ar_condicionado") AS BOOL) indicador_ar_condicionado,
   SAFE_CAST(JSON_VALUE(content,"$.indicador_elevador") AS BOOL) indicador_elevador,
   SAFE_CAST(JSON_VALUE(content,"$.indicador_usb") AS BOOL) indicador_usb,
   SAFE_CAST(JSON_VALUE(content,"$.indicador_wifi") AS BOOL) indicador_wifi,
   SAFE_CAST(JSON_VALUE(content,"$.nome_chassi") AS STRING) nome_chassi,
   SAFE_CAST(JSON_VALUE(content,"$.permissao") AS STRING) permissao,
   SAFE_CAST(JSON_VALUE(content,"$.placa") AS STRING) placa,
   SAFE_CAST(JSON_VALUE(content,"$.quantidade_lotacao_pe") AS INT64) quantidade_lotacao_pe,
   SAFE_CAST(JSON_VALUE(content,"$.quantidade_lotacao_sentado") AS INT64) quantidade_lotacao_sentado,
   SAFE_CAST(JSON_VALUE(content,"$.tipo_combustivel") AS STRING) tipo_combustivel,
   SAFE_CAST(JSON_VALUE(content,"$.tipo_veiculo") AS STRING) tipo_veiculo,
   SAFE_CAST(JSON_VALUE(content,"$.status") AS STRING) status,
   CASE
    WHEN JSON_VALUE(content,"$.data_inicio_vinculo") = "" THEN NULL
    ELSE
    SAFE_CAST(PARSE_DATETIME("%d/%m/%Y", JSON_VALUE(content,"$.data_inicio_vinculo")) AS DATE)
  END
    AS data_inicio_vinculo,
 FROM
    {{ source('veiculo_staging','sppo_licenciamento_stu') }} as t