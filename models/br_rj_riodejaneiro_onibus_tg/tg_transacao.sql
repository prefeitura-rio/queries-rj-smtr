{{ 
  config(alias='transacao') 
}}

SELECT
  SAFE_CAST(tipo_registro AS STRING) AS tipo_registro,
  SAFE_CAST(versao_registro AS STRING) AS versao_registro,
  SAFE_CAST(DATETIME(TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE '2002-12-31', INTERVAL data DAY)), INTERVAL hora SECOND), "America/Sao_Paulo") AS DATETIME) AS timestamp_transacao,
  SAFE_CAST(emissor_aplicacao || "_" || aplicacao AS STRING) AS numero_aplicacao,
  SAFE_CAST(tsn AS INT64) AS sequencial_transacao,
  SAFE_CAST(status AS INT64) AS status,
  SAFE_CAST(num_onibus AS STRING) AS id_veiculo,
  SAFE_CAST(tipo_embarque AS INT64) AS tipo_embarque,
  SAFE_CAST(SAFE_CAST(tipo_debito AS FLOAT64) AS INT64) AS tipo_debito,
  SAFE_CAST(SAFE_CAST(mensagem_debito AS FLOAT64) AS INT64) AS mensagem_debito,
  SAFE_CAST(valor_tarifa/100 AS FLOAT64) AS valor_tarifa,
  SAFE_CAST(valor_tarifa_anterior/100 AS FLOAT64) AS valor_tarifa_anterior,
  SAFE_CAST(valor_debitado/100 AS FLOAT64) AS valor_debitado,
  SAFE_CAST(COALESCE(SAFE_CAST(diferenca_valor_debitado AS INT64)/100, 0) AS FLOAT64) AS diferenca_valor_debitado,
  SAFE_CAST(SAFE_CAST(valor_promo_desconto AS INT64)/100 AS FLOAT64) AS valor_promo_desconto,
  SAFE_CAST(SAFE_CAST(valor_acumulado AS INT64)/100 AS FLOAT64) AS valor_acumulado,
  SAFE_CAST(origin_file AS STRING) AS arquivo_origem,
  SAFE_CAST(garagem AS STRING) AS garagem,
  SAFE_CAST(file_date AS STRING) AS data_arquivo_origem,
  SAFE_CAST(codigo_empresa AS INT64) AS codigo_empresa,
  SAFE_CAST(uid AS STRING) AS id_cartao,
  SAFE_CAST(ano AS INT64) AS ano,
  SAFE_CAST(mes AS INT64) AS mes,
  SAFE_CAST(dia AS INT64) AS dia
FROM
  {{var('tg_transacao_staging')}}
WHERE
  (ano BETWEEN 2022 AND 2023) -- Apenas dados de 2022 e 2023
  AND mes >= 1
  AND dia >= 1
  AND status = 0 -- Apenas transações com sucesso
  AND (emissor_aplicacao BETWEEN 1 AND 15)
  AND (aplicacao BETWEEN 0 AND 1023)