SELECT 
  SAFE_CAST(veiculo AS STRING) veiculo,
  SAFE_CAST(dataOperacao AS DATETIME) data_operacao,
  SAFE_CAST(linha AS STRING) linha,
  SAFE_CAST(dataEntrada AS DATETIME) data_entrada,
  SAFE_CAST(dataSaida AS DATETIME) data_saida,
  SAFE_CAST(dataProcessado AS DATETIME) data_processado,
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(hora AS INT64) hora
FROM
  {{var('sppo_registros_realocacao_staging')}} as t