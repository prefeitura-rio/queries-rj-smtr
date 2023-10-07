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
  viagens_planejadas AS (
  SELECT
    data_versao,
    servico,
    tipo_dia,
    viagens_planejadas
  FROM
      {{ ref("subsidio_ordem_servico") }}
  WHERE
    data_versao BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
  ),
  data_versao_efetiva AS (
  SELECT
    data,
    tipo_dia,
    COALESCE(data_versao_trips, data_versao_shapes, data_versao_frequencies) AS data_versao
  FROM
      {{ ref("subsidio_data_versao_efetiva") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
  ),
  viagem_planejada AS (
  SELECT
    p.*,
    v.viagens_planejadas
  FROM
    planejado AS p
  LEFT JOIN
    data_versao_efetiva AS d
  USING
    (data, tipo_dia)
  LEFT JOIN
    viagens_planejadas AS v
  ON
    d.data_versao = v.data_versao
    AND v.tipo_dia = v.tipo_dia
    AND p.servico = v.servico
  ),
-- 2. Status dos veículos
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
-- 3. Viagens realizadas
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
-- 4. Parâmetros de subsídio
  subsidio_parametros AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    status,
    subsidio_km
  FROM
    `rj-smtr`.`dashboard_subsidio_sppo`.`subsidio_parametros` ),
-- 5. Viagens com tipo e valor de subsídio por km
  viagem_km_tipo AS (
  SELECT
    v.DATA,
    v.servico,
    ve.status AS tipo_viagem,
    id_viagem,
    distancia_planejada,
    t.subsidio_km
  FROM
    viagem AS v
  LEFT JOIN
    veiculos AS ve
  USING
    (DATA,
      id_veiculo)
  LEFT JOIN
    subsidio_parametros AS t
  ON
    v.data BETWEEN t.data_inicio
    AND t.data_fim
    AND ve.status = t.status ),
-- 6. Apuração de km realizado e Percentual de Operação Diário (POD)
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
    viagem_planejada AS p
  LEFT JOIN
    viagem_km_tipo AS v
  USING
    (DATA,
      servico)
  GROUP BY
    1,
    2,
    3,
    4,
    5
  ORDER BY
    DATA,
    servico ),
-- 7. Filtro de viagens que serão consideradas para fins de apuração de valor de subsídio
  viagem_subsidio_apuracao AS (
  SELECT
    v.* EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY DATA, servico ORDER BY subsidio_km DESC, distancia_planejada DESC) AS rn
    FROM
      viagem_km_tipo ) AS v
  LEFT JOIN
    viagem_planejada AS p
  USING
    (DATA,
      servico)
  LEFT JOIN
    servico_km_apuracao AS s
  USING
    (DATA,
      servico)
  WHERE
    CASE
      WHEN p.tipo_dia = "Dia Útil" AND viagens_planejadas > 10 AND perc_km_planejada > 120 AND rn > viagens_planejadas*1.2 THEN FALSE
      WHEN p.tipo_dia = "Dia Útil" AND viagens_planejadas <= 10 AND perc_km_planejada > 200 AND rn > viagens_planejadas*2 THEN FALSE
    ELSE
    TRUE
  END = TRUE ),
-- 8. Apuração de valor de subsídio por data e serviço
  servico_subsidio_apuracao AS (
  SELECT
    DATA,
    servico,
    SUM(distancia_planejada*subsidio_km) AS valor_subsidio_apurado
  FROM
    viagem_subsidio_apuracao
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