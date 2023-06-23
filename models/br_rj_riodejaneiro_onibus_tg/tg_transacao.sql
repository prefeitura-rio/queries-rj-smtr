{{ 
  config( 
    alias="transacao",
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  ) 
}}

WITH
  tg_all_transactions AS (
  SELECT
    * EXCEPT(secao_entrada,
      secao_saida),
    SAFE_CAST(secao_entrada AS STRING) AS secao_entrada,
    SAFE_CAST(secao_saida AS STRING) AS secao_saida
  FROM
    `rj-smtr-staging.br_rj_riodejaneiro_onibus_tg.card_transactions`
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

    {% if var("date_range_start") == "None" %}

    {% set date_range_start = run_query("SELECT gr FROM (SELECT IF(MAX(data) > DATE('" ~ var("date_range_end") ~ "'), DATE('" ~ var("date_range_end") ~ "'), MAX(data)) AS gr FROM " ~ this ~ ")").columns[0].values()[0] %}

    {% else %}

    {% set date_range_start = var("date_range_start") %}

    {% endif %}

    AND dia >= EXTRACT(DAY FROM DATE("{{ date_range_start }}"))
    AND mes >= EXTRACT(MONTH FROM DATE("{{ date_range_start }}"))
    AND ano >= EXTRACT(YEAR FROM DATE("{{ date_range_start }}"))
  ),
  tg_rn_transactions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY tipo_registro, versao_registro, DATA, hora, numero_interno, aplicacao, emissor_aplicacao, tsn, valor_tarifa, valor_tarifa_anterior, valor_debitado, status, num_onibus, mot_tsp, mot_codigo_pro_data, mot_matricula, cob_tsp, cob_codigo_pro_data, cob_matricula, tipo_embarque, serial_number_sam_val, l_sequence_number, assinatura, diferenca_valor_debitado, secao_entrada, secao_saida, avl_status, provider_id, ext_use_ctr, ext_val_date, valor_promo_desconto, valor_acumulado, tipo_debito, mensagem_debito, tp_credit_purse_a, tp_credit_purse_b, csn_purse_a, csn_purse_b, origin_file, file_date, codigo_empresa, tsn_date, uid, ano, mes, dia ) AS rn
  FROM
    tg_all_transactions ),
  tg_filtered_transactions AS (
  SELECT
    SAFE_CAST(DATE_ADD(DATE '2002-12-31', INTERVAL DATA DAY) AS DATE) AS DATA,
    SAFE_CAST(codigo_empresa AS STRING) AS id_empresa,
    SAFE_CAST(num_onibus AS STRING) AS id_veiculo,
    SAFE_CAST(uid AS STRING) AS id_cartao,
    SAFE_CAST(emissor_aplicacao || "_" || aplicacao AS STRING) AS tipo_cartao,
    SAFE_CAST(tsn AS STRING) AS sequencial_transacao_cartao,
    SAFE_CAST(DATETIME(TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE '2002-12-31', INTERVAL DATA DAY)), INTERVAL hora SECOND), "America/Sao_Paulo") AS DATETIME) AS datetime,
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
    tg_rn_transactions
  WHERE
    rn = 1 )
-- TODO: Filtrar apenas registros que tenham divergência exclusivamente na coluna valor_debitado para realizar a soma
SELECT
  DATA,
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
  SUM(debito) AS debito,
  desconto,
  total_integracao
FROM
  tg_filtered_transactions
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  14,
  15
