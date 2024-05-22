{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "servico"],
        incremental_strategy="insert_overwrite",
    )
}}


WITH
-- 1. Viagens planejadas (agrupadas por data e serviço)
  planejado AS (
  SELECT
    DISTINCT DATA,
    tipo_dia,
    consorcio,
    servico,
    distancia_total_planejada AS km_planejada,
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    DATA BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
    AND ( distancia_total_planejada > 0
      OR distancia_total_planejada IS NULL )
  ),
-- 2. Viagens realizadas
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
-- 3. Apuração de km realizado e Percentual de Operação Diário (POD)
  servico_km_apuracao AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.consorcio,
    p.servico,
    p.km_planejada AS km_planejada,
    COALESCE(COUNT(v.id_viagem), 0) AS viagens,
    COALESCE(SUM(v.distancia_planejada), 0) AS km_apurada,
    COALESCE(ROUND(100 * SUM(v.distancia_planejada) / p.km_planejada,2), 0) AS perc_km_planejada
  FROM
    planejado AS p
  LEFT JOIN
    viagem AS v
  USING
    (DATA,
      servico)
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
-- 4. Apuração de valor de subsídio por data e serviço
  viagens_remuneradas AS (
    SELECT
      DATA,
      servico,
      distancia_planejada,
      subsidio_km
    FROM
      {{ ref("viagens_remuneradas") }}
    WHERE
      DATA BETWEEN DATE("{{ var("start_date") }}")
      AND DATE( "{{ var("end_date") }}" )
      AND indicador_viagem_remunerada IS TRUE),
  servico_subsidio_apuracao AS (
  SELECT
    DATA,
    servico,
    SUM(distancia_planejada*subsidio_km) AS valor_subsidio_apurado
  FROM
    viagens_remuneradas
  GROUP BY
    1,
    2)
SELECT
  s.*,
  IF(p.valor IS NULL, st.valor_subsidio_apurado, 0) AS valor_subsidio_pago,
  IFNULL(-p.valor, 0) AS valor_penalidade
FROM
  servico_km_apuracao AS s
LEFT JOIN
  {{ ref("valor_tipo_penalidade") }} AS p
ON
  s.data BETWEEN p.data_inicio
  AND p.data_fim
  AND s.perc_km_planejada >= p.perc_km_inferior
  AND s.perc_km_planejada < p.perc_km_superior
LEFT JOIN
  servico_subsidio_apuracao AS st
USING
  (DATA,
    servico)
