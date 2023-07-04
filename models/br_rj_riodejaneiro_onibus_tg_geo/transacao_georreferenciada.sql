/*  Query para criar a view com georreferenciamento dos dados do TG*/ 

SELECT 
    t1.DATA,
    t1.id_empresa,
    t1.id_veiculo_t1 as id_veiculo,  -- rename back to original name
    t1.id_cartao,
    t1.tipo_cartao,
    t1.sequencial_transacao_cartao,
    t1.timestamp_transacao,
    t1.tipo_embarque,
    t1.tipo_debito,
    t1.mensagem_debito,
    t1.tarifa,
    t1.tarifa_anterior,
    t1.debito,
    t1.desconto,
    t1.total_integracao,
    t1.datetime_minuto as datetime_minuto,  -- rename back to original name
    t2.servico,
    t2.latitude,
    t2.longitude
FROM (
    SELECT
      DATA,
      id_empresa,
      id_veiculo as id_veiculo_t1,  -- temporary alias to avoid duplication
      id_cartao,
      id_tipo_cartao AS tipo_cartao,
      sequencial_transacao_veiculo AS sequencial_transacao_cartao,
      timestamp_transacao,
      tipo_embarque,
      tipo_debito,
      mensagem_debito,
      MAX(valor_tarifa) AS tarifa,
      MAX(valor_tarifa_anterior) AS tarifa_anterior,
      SUM(valor_debitado) AS debito,
      SUM(valor_promo_desconto) AS desconto,
      SUM(valor_total_integracao) AS total_integracao,
      FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M", DATE_TRUNC(timestamp_transacao, MINUTE)) AS datetime_minuto
    FROM
      `rj-smtr.br_rj_riodejaneiro_onibus_tg.transacao`
    WHERE
      DATA = DATE( "{{ var("run_date") }}" ) 
    GROUP BY
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    HAVING
      COUNT(DISTINCT CONCAT(id_cartao, datetime_minuto)) <= 4
) AS t1
LEFT JOIN (
  SELECT
    id_veiculo,  -- here, the column keeps its original name
    datetime_minuto,
    servico,
    ROUND(AVG(latitude), 3) as latitude,
    ROUND(AVG(longitude), 3) as longitude
  FROM (
    SELECT DISTINCT
      CASE WHEN REGEXP_CONTAINS(id_veiculo, r"^[A-Za-z]") THEN SUBSTR(id_veiculo, 2) ELSE id_veiculo END AS id_veiculo,
      servico,
      latitude,
      longitude,
      FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M", DATE_TRUNC(timestamp_gps, MINUTE)) AS datetime_minuto
    FROM
      `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    WHERE
      data = DATE( "{{ var("run_date") }}" ) 
  )
  GROUP BY
    id_veiculo, datetime_minuto, servico
) AS t2
ON t1.id_veiculo_t1 = t2.id_veiculo AND t1.datetime_minuto = t2.datetime_minuto
WHERE t2.latitude IS NOT NULL