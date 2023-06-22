{{ 
  config( 
    alias="transacao",
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  ) 
}}

SELECT
  SAFE_CAST(DATE_ADD(DATE '2002-12-31', INTERVAL data DAY) AS DATE) AS data,
  SAFE_CAST(codigo_empresa AS STRING) AS id_empresa,
  SAFE_CAST(num_onibus AS STRING) AS id_veiculo,
  SAFE_CAST(uid AS STRING) AS id_cartao, 
  SAFE_CAST(emissor_aplicacao || "_" || aplicacao AS STRING) AS tipo_cartao,
  SAFE_CAST(tsn AS STRING) AS sequencial_transacao_cartao,
  SAFE_CAST(DATETIME(TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE '2002-12-31', INTERVAL data DAY)), INTERVAL hora SECOND), "America/Sao_Paulo") AS DATETIME) AS timestamp_transacao,
  SAFE_CAST(tipo_embarque AS STRING) AS tipo_embarque,
  SAFE_CAST(SAFE_CAST(tipo_debito AS FLOAT64) AS STRING) AS tipo_debito,
  SAFE_CAST(SAFE_CAST(mensagem_debito AS FLOAT64) AS STRING) AS mensagem_debito,
  SAFE_CAST(valor_tarifa/100 AS FLOAT64) AS tarifa,
  SAFE_CAST(valor_tarifa_anterior/100 AS FLOAT64) AS tarifa_anterior,
  SAFE_CAST(valor_debitado/100 AS FLOAT64) AS debito,
  SAFE_CAST(SAFE_CAST(valor_promo_desconto AS INT64)/100 AS FLOAT64) AS desconto,
  SAFE_CAST(SAFE_CAST(valor_acumulado AS INT64)/100 AS FLOAT64) AS total_integracao 
FROM
  {{var('tg_transacao_staging')}}
WHERE
  (ano BETWEEN 2022 AND 2023) -- Apenas dados de 2022 e 2023
  AND mes >= 1
  AND dia >= 1
  AND status = 0 -- Apenas transações com sucesso
  AND (emissor_aplicacao BETWEEN 1 AND 15)
  AND (aplicacao BETWEEN 0 AND 1023)
  AND dia <= EXTRACT(DAY FROM DATE("{{ var("date_range_end") }}"))
  AND mes <= EXTRACT(MONTH FROM DATE("{{ var("date_range_end") }}"))
  AND ano <= EXTRACT(YEAR FROM DATE("{{ var("date_range_end") }}"))

  {% if is_incremental() %}

  {% if var("date_range_start") == "None" %}

  {% set date_range_start = run_query("SELECT gr FROM (SELECT IF(MAX(data) > DATE('" ~ var("date_range_end") ~ "'), DATE('" ~ var("date_range_end") ~ "'), MAX(data)) AS gr FROM " ~ this ~ ")").columns[0].values()[0] %}

  {% else %}

  {% set date_range_start = var("date_range_start") %}

  {% endif %}

    AND dia >= EXTRACT(DAY FROM DATE("{{ date_range_start }}"))
    AND mes >= EXTRACT(MONTH FROM DATE("{{ date_range_start }}"))
    AND ano >= EXTRACT(YEAR FROM DATE("{{ date_range_start }}"))

  {% endif %}