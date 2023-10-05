{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "servico"],
        incremental_strategy="insert_overwrite",
    )
}}


WITH
  planejado AS (
  SELECT
    DISTINCT DATA,
    tipo_dia,
    consorcio,
    servico,
    MAX(distancia_total_planejada) AS km_planejada,
    ROUND(MAX(distancia_total_planejada)/SUM(distancia_planejada), 2) AS viagens_planejadas
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    DATA BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
    AND ( distancia_total_planejada > 0
      OR distancia_total_planejada IS NULL )
  GROUP BY
    1,
    2,
    3,
    4),
  veiculos AS (
  SELECT
    DATA,
    id_veiculo,
    status
  FROM
    {{ ref("sppo_veiculo_dia") }}
  WHERE
    DATA BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}") ),
  viagem AS (
  SELECT
    DATA,
    servico_realizado AS servico,
    id_veiculo,
    id_viagem,
    distancia_planejada
  FROM
    {{ ref("viagem_completa") }}
  WHERE
    DATA BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" ) ),
  servico_km_tipo AS (
  SELECT
    v.DATA,
    v.servico,
    ve.status AS tipo_viagem,
    COUNT(id_viagem) AS viagens,
    SUM(distancia_planejada) AS km_apurada
  FROM
    viagem AS v
  LEFT JOIN
    veiculos AS ve
  USING
    (DATA,
      id_veiculo)
  GROUP BY
    1,
    2,
    3 ),
  subsidio_km_tipo AS (
  SELECT
    DISTINCT v.*,
    v.km_apurada * t.subsidio_km AS valor_subsidio_apurado
  FROM
    servico_km_tipo AS v
  LEFT JOIN
    {{ ref("subsidio_parametros") }} AS t
  ON
    v.data BETWEEN t.data_inicio
    AND t.data_fim
    AND v.tipo_viagem = t.status ),
  servico_km AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.consorcio,
    p.servico,
    COALESCE(SUM(v.viagens), 0) AS viagens,
    COALESCE(ROUND(SUM(v.km_apurada), 2), 0) AS km_apurada,
    p.km_planejada AS km_planejada,
    COALESCE(ROUND(100 * SUM(v.km_apurada) / p.km_planejada, 2), 0) AS perc_km_planejada
  FROM
    planejado p
  LEFT JOIN
    servico_km_tipo AS v
  USING
    (DATA,
      servico)
  GROUP BY
    1,
    2,
    3,
    4,
    7 ),
  subsidio_valor_apurado AS (
  SELECT
    DATA,
    servico,
    SUM(valor_subsidio_apurado) AS valor_subsidio_apurado
  FROM
    subsidio_km_tipo
  GROUP BY
    1,
    2 ),
  subsidio_sumario AS (
  SELECT
    s.*,
  IF
    (p.valor IS NULL, st.valor_subsidio_apurado, 0) AS valor_subsidio_pago,
    IFNULL(-p.valor, 0) AS valor_penalidade
  FROM
    servico_km AS s
  LEFT JOIN
    {{ ref("valor_tipo_penalidade") }} AS p
  ON
    s.data BETWEEN p.data_inicio
    AND p.data_fim
    AND s.perc_km_planejada >= p.perc_km_inferior
    AND s.perc_km_planejada < p.perc_km_superior
  LEFT JOIN
    subsidio_valor_apurado AS st
  USING
    (DATA,
      servico)),
  subsidio_parametro_ajuste AS (
  SELECT
    s.*,
    CASE
      WHEN s.tipo_dia = "Dia Útil" AND perc_km_planejada > 120 AND viagens_planejadas > 10 THEN 120/perc_km_planejada
      WHEN s.tipo_dia = "Dia Útil" AND perc_km_planejada > 200 AND viagens_planejadas <= 10 THEN 200/perc_km_planejada
    ELSE
    1
  END
    AS parametro_ajuste
  FROM
    subsidio_sumario AS s
  LEFT JOIN
    planejado AS p
  USING
    (DATA,
      servico))
SELECT
  *,
  valor_subsidio_pago*parametro_ajuste AS valor_subsidio_pago
FROM
  subsidio_parametro_ajuste