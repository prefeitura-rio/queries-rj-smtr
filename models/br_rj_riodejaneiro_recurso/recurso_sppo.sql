{{config(
  partition_by = { "field" :"data",
   "data_type" :"date",
   "granularity": "day" },
    unique_key = ["protocol", "data"],
    alias = "recurso_sppo",
)}}

SELECT
  SAFE_CAST(protocol AS STRING) AS protocol,
  SAFE_CAST(protocol AS DATE) AS data,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(content, "$.createdDate") AS DATE
  ) AS data_ticket,
  SAFE_CAST(JSON_EXTRACT_SCALAR(content, "$.id") AS STRING) AS id_ticket,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111870']"
    ) AS STRING
  ) AS consorcio,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111871']"
    ) AS INT64
  ) AS linha,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111872']"
    ) AS STRING
  ) AS servico,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111873']"
    ) AS INT64
  ) AS numero_ordem_veiculo,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111901']"
    ) AS STRING
  ) AS direcao_servico,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111866']"
    ) AS STRING
  ) AS motivo,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111867']"
    ) AS DATE
  ) AS dia_viagem,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111868']"
    ) AS STRING
  ) AS hora_inicio_viagem,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0].['customFieldId:111869']"
    ) AS STRING
  ) AS hora_fim_viagem,
  SAFE_CAST(timestamp_captura AS STRING) AS timestamp_captura,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0]['customFieldId:111865']"
    ) AS STRING
  ) AS julgamento,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0]['customFieldId == 111904']"
    ) AS STRING
  ) AS motivo_indeferido,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(
      content,
      "$.customFieldValues[0].items[0]['customFieldId == 125615']"
    ) AS STRING
  ) AS observacao

  FROM {{source(
     "br_rj_riodejaneiro_recurso_staging",
     "recurso_sppo"
  )}}


# rever o json_extract
    
