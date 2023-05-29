{{ 
  config(alias='tg_card_transactions') 
}}

SELECT
  SAFE_CAST(tipo_registro AS STRING) AS tipo_registro,
  SAFE_CAST(versao_registro AS STRING) AS versao_registro,
  SAFE_CAST(DATETIME(TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE '2002-12-31', INTERVAL data DAY)), INTERVAL hora SECOND), "America/Sao_Paulo") AS DATETIME) AS tsn_timestamp,
  SAFE_CAST(aplicacao AS INT64) AS aplicacao,
  SAFE_CAST(emissor_aplicacao AS INT64) AS emissor_aplicacao,
  SAFE_CAST(tsn AS INT64) AS tsn,
  SAFE_CAST(valor_tarifa/100 AS FLOAT64) AS valor_tarifa,
  SAFE_CAST(valor_tarifa_anterior/100 AS FLOAT64) AS valor_tarifa_anterior,
  SAFE_CAST(valor_debitado/100 AS FLOAT64) AS valor_debitado,
  SAFE_CAST(status AS INT64) AS status,
  SAFE_CAST(num_onibus AS STRING) AS id_veiculo,
  SAFE_CAST(tipo_embarque AS INT64) AS tipo_embarque,
  SAFE_CAST(SAFE_CAST(diferenca_valor_debitado AS INT64)/100 AS FLOAT64) AS diferenca_valor_debitado,
  SAFE_CAST(SAFE_CAST(valor_promo_desconto AS INT64)/100 AS FLOAT64) AS valor_promo_desconto,
  SAFE_CAST(SAFE_CAST(valor_acumulado AS INT64)/100 AS FLOAT64) AS valor_acumulado,
  SAFE_CAST(SAFE_CAST(tipo_debito AS FLOAT64) AS INT64) AS tipo_debito,
  SAFE_CAST(SAFE_CAST(mensagem_debito AS FLOAT64) AS INT64) AS mensagem_debito,
  SAFE_CAST(origin_file AS STRING) AS origin_file,
  SAFE_CAST(garagem AS STRING) AS garagem,
  SAFE_CAST(file_date AS STRING) AS file_date,
  SAFE_CAST(codigo_empresa AS INT64) AS codigo_empresa,
  SAFE_CAST(uid AS STRING) AS uid,
  SAFE_CAST(ano AS INT64) AS ano,
  SAFE_CAST(mes AS INT64) AS mes,
  SAFE_CAST(dia AS INT64) AS dia
FROM
  {{var('tg_card_transactions_staging')}}
WHERE
  ano = 2023
  AND mes = 4
  AND dia = 1
LIMIT
  10