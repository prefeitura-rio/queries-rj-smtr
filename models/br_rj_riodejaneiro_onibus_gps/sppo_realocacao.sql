SELECT 
  SAFE_CAST(id_veiculo AS STRING) id_veiculo,
  SAFE_CAST(datetime_operacao AS DATETIME) datetime_operacao,
  concat(
    ifnull(REGEXP_EXTRACT(servico, r'[A-Z]+'), ""), 
    ifnull(REGEXP_EXTRACT(servico, r'[0-9]+'), "") 
  ) as servico,
  SAFE_CAST(datetime_entrada AS DATETIME) as datetime_entrada,
  SAFE_CAST(datetime_saida AS DATETIME) as datetime_saida,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_processamento), "America/Sao_Paulo") AS DATETIME) as timestamp_processamento,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) as timestamp_captura,
  data,
  hora
FROM
  {{var('sppo_realocacao_staging')}} as t