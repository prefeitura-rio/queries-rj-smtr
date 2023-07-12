{{ 
  config( 
    alias="transacao",
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  ) 
}}

WITH
  tg AS (
  SELECT
    SAFE_CAST(DATE_ADD(DATE '2002-12-31', INTERVAL DATA DAY) AS DATE) AS DATA,
    SAFE_CAST(codigo_empresa AS STRING) AS id_empresa,
    SAFE_CAST(num_onibus AS STRING) AS id_veiculo,
    SAFE_CAST(uid AS STRING) AS id_cartao,
    SAFE_CAST(emissor_aplicacao || "_" || aplicacao AS STRING) AS tipo_cartao,
    SAFE_CAST(tsn AS STRING) AS sequencial_transacao_cartao,
    SAFE_CAST(DATETIME(TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE '2002-12-31', INTERVAL DATA DAY)), INTERVAL hora SECOND)) AS DATETIME) AS datetime,
    SAFE_CAST(tipo_embarque AS STRING) AS tipo_embarque,
    SAFE_CAST(SAFE_CAST(tipo_debito AS FLOAT64) AS STRING) AS tipo_debito,
    SAFE_CAST(SAFE_CAST(mensagem_debito AS FLOAT64) AS STRING) AS mensagem_debito,
    SAFE_CAST(valor_tarifa/100 AS FLOAT64) AS tarifa,
    SAFE_CAST(valor_tarifa_anterior/100 AS FLOAT64) AS tarifa_anterior,
    SAFE_CAST(valor_debitado/100 AS FLOAT64) AS debito,
    SAFE_CAST(SAFE_CAST(valor_promo_desconto AS INT64)/100 AS FLOAT64) AS desconto,
    SAFE_CAST(SAFE_CAST(valor_acumulado AS INT64)/100 AS FLOAT64) AS total_integracao,
    SAFE_CAST(garagem AS STRING) AS garagem
  FROM
    {{ var('tg_transacao_staging') }}
  WHERE
    {% if var("date_range_start") == "None" %}

    {% set date_range_start = "2022-06-01" %}

    {% else %}

    {% set date_range_start = var("date_range_start") %}

    {% endif %}

    ano BETWEEN EXTRACT(YEAR FROM DATE("{{ date_range_start }}")) AND EXTRACT(YEAR FROM DATE("{{ var("date_range_end") }}"))
    AND mes BETWEEN EXTRACT(MONTH FROM DATE("{{ date_range_start }}")) AND EXTRACT(MONTH FROM DATE("{{ var("date_range_end") }}"))
    AND dia BETWEEN EXTRACT(DAY FROM DATE("{{ date_range_start }}")) AND EXTRACT(DAY FROM DATE("{{ var("date_range_end") }}"))
    AND status = 0 -- Apenas transações com sucesso
    AND (emissor_aplicacao BETWEEN 1 AND 15) -- Range de emissores de cartão válidos (fonte: RioCard)
    AND (aplicacao BETWEEN 0 AND 1023) -- Range de aplicações válidas (fonte: RioCard)

  ),
  tg_soma_debito AS ( 
  SELECT
    data,
    id_empresa,
    id_veiculo,
    id_cartao,
    tipo_cartao,
    sequencial_transacao_cartao,
    datetime,
    tipo_embarque,
    tipo_debito,
    mensagem_debito,
    tarifa,
    tarifa_anterior,
    ROUND(SUM(debito), 2) AS debito,
    desconto,
    total_integracao,
    garagem
  FROM
    tg
  GROUP BY
    data,
    id_empresa,
    id_veiculo,
    id_cartao,
    tipo_cartao,
    sequencial_transacao_cartao,
    datetime,
    tipo_embarque,
    tipo_debito,
    mensagem_debito,
    tarifa,
    tarifa_anterior,
    desconto,
    total_integracao,
    garagem),
  tg_rn_garagem AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY datetime, id_cartao ) AS rn_garagem
  FROM
    tg_soma_debito )
SELECT
  * EXCEPT(garagem,
    rn_garagem)
FROM
  tg_rn_garagem
WHERE
  rn_garagem = 1