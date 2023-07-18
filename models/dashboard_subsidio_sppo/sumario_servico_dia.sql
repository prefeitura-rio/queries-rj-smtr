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
    distancia_total_planejada AS km_planejada
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    DATA BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
    AND distancia_total_planejada > 0 ),
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
    ROUND(SUM(distancia_planejada), 2) AS km_apurada
  FROM
    viagem v
  LEFT JOIN
    veiculos ve
  ON
    ve.data = v.data
    AND ve.id_veiculo = v.id_veiculo
  GROUP BY
    1,
    2,
    3 ),
  subsidio_km_tipo AS (
  SELECT
    v.*,
    round(v.km_apurada * t.valor_km, 2) AS valor_subsidio_apurado
  FROM
    servico_km_tipo v
  LEFT JOIN
    {{ ref("valor_tipo_viagem") }} t
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
    IFNULL(SUM(v.viagens), 0) AS viagens,
    IFNULL(ROUND(SUM(v.km_apurada), 2), 0) AS km_apurada,
    p.km_planejada AS km_planejada,
    IFNULL(ROUND(100 * SUM(v.km_apurada) / p.km_planejada, 2), 0) AS perc_km_planejada
  FROM
    planejado p
  LEFT JOIN
    servico_km_tipo v
  ON
    p.data = v.data
    AND p.servico = v.servico
  GROUP BY
    1,
    2,
    3,
    4,
    7 )
SELECT
  s.*,
  IF(p.valor IS NULL, st.valor_subsidio_apurado, 0) AS valor_subsidio_pago,
  IFNULL(-p.valor, 0) AS valor_penalidade,
  "{{ var("version") }}" as versao
FROM
  servico_km s
LEFT JOIN
  {{ ref("valor_tipo_penalidade") }} p
ON
  s.data BETWEEN p.data_inicio
  AND p.data_fim
  AND s.perc_km_planejada >= p.perc_km_inferior
  AND s.perc_km_planejada < p.perc_km_superior
LEFT JOIN (
  SELECT
    DATA,
    servico,
    SUM(valor_subsidio_apurado) AS valor_subsidio_apurado
  FROM
    subsidio_km_tipo
  GROUP BY
    1,
    2 ) st
ON
  s.data = st.data
  AND s.servico = st.servico