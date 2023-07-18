-- Gabarito v1 + v2
WITH
  planejado AS (
  SELECT
    DISTINCT `data`,
    tipo_dia,
    consorcio,
    servico
  FROM
    {{ ref("viagem_planejada") }} -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
  WHERE
    `data` <= DATE( "{{ var("end_date") }}" )
    AND distancia_total_planejada IS NOT NULL ),
  planejado_sumario_dia AS (
  SELECT
    consorcio,
    DATA,
    tipo_dia,
    trip_id_planejado AS trip_id,
    servico,
    sentido,
    CASE
      WHEN sentido = "C" THEN MAX(distancia_planejada)
    ELSE
    SUM(distancia_planejada)
  END
    AS distancia_planejada,
    MAX(distancia_total_planejada) AS distancia_total_planejada,
    NULL AS viagens_planejadas
  FROM
    {{ ref("viagem_planejada") }} -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
  WHERE
    DATA >= "2022-06-01"
    AND DATA < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND distancia_total_planejada > 0
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
  viagem_sumario_dia AS (
  SELECT
    DATA,
    trip_id,
    COUNT(id_viagem) AS viagens_realizadas
  FROM
    {{ ref("viagem_completa") }} -- `rj-smtr.projeto_subsidio_sppo.viagem_completa`
  WHERE
    DATA >= "2022-06-01"
    AND DATA < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
  GROUP BY
    1,
    2),
  previa_sumario_dia AS (
  SELECT
    consorcio,
    DATA,
    tipo_dia,
    servico,
    distancia_planejada,
    NULL AS viagens_planejadas,
    IFNULL(SUM(v.viagens_realizadas), 0) AS viagens_subsidio,
    distancia_total_planejada,
    (IFNULL(SUM(v.viagens_realizadas), 0) * distancia_planejada) AS distancia_total_subsidio
  FROM
    planejado_sumario_dia AS p
  LEFT JOIN
    viagem_sumario_dia AS v
  USING
    (trip_id,
      DATA)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    8 ),
  sumario_dia_agg AS (
  SELECT
    consorcio,
    DATA,
    tipo_dia,
    servico,
    NULL AS viagens_planejadas,
    IFNULL(SUM(viagens_subsidio), 0) AS viagens_subsidio,
    distancia_total_planejada,
    ROUND(SUM(distancia_total_subsidio), 3) AS distancia_total_subsidio
  FROM
    previa_sumario_dia
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    7 ),
  valor_sumario_dia AS (
  SELECT
    s.*,
    v.valor_subsidio_por_km,
    ROUND(distancia_total_subsidio * v.valor_subsidio_por_km, 2) AS valor_total_aferido,
  IF
    (distancia_total_planejada = 0, NULL, ROUND(100*distancia_total_subsidio/distancia_total_planejada, 2)) AS perc_distancia_total_subsidio
  FROM
    sumario_dia_agg s
  LEFT JOIN (
    SELECT
      *
    FROM
      {{ ref("subsidio_data_versao_efetiva") }} -- `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
    WHERE
      DATA >= "2022-06-01"
      AND DATA < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )) AS v
  ON
    v.data = s.data ),
  sumario_dia AS (
  SELECT
    *,
    CASE
      WHEN (perc_distancia_total_subsidio < 80) OR (perc_distancia_total_subsidio IS NULL) THEN 0
    ELSE
    valor_total_aferido
  END
    AS valor_total_subsidio
  FROM
    valor_sumario_dia),
  sumario_v1 AS ( -- Viagens v1
  SELECT
    `data`,
    servico,
    "Não classificado" AS tipo_viagem,
    viagens_subsidio AS viagens,
    ROUND(distancia_total_subsidio, 2) AS km_apurada
  FROM
     sumario_dia
  WHERE
    `data` < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ) ),
  tipo_viagem_v2 AS ( -- Classifica os tipos de viagem (v2)
  SELECT
    `data`,
    id_veiculo,
    status
  FROM
      {{ ref("sppo_veiculo_dia") }} -- `rj-smtr`.`veiculo`.`sppo_veiculo_dia`
  WHERE
    `data` BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE( "{{ var("end_date") }}" ) ),
  viagem_v2 AS (
  SELECT
    `data`,
    servico_realizado AS servico,
    id_veiculo,
    id_viagem,
    distancia_planejada
  FROM
    {{ ref("viagem_completa") }} -- `rj-smtr.projeto_subsidio_sppo.viagem_completa`
  WHERE
    `data` BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE( "{{ var("end_date") }}" ) ),
  sumario_v2 AS (
  SELECT
    v.`data`,
    v.servico,
    ve.status AS tipo_viagem,
    COUNT(id_viagem) AS viagens,
    ROUND(SUM(distancia_planejada), 2) AS km_apurada
  FROM
    viagem_v2 v
  LEFT JOIN
    tipo_viagem_v2 ve
  ON
    ve.`data` = v.`data`
    AND ve.id_veiculo = v.id_veiculo
  GROUP BY
    1,
    2,
    3 ) -- Todas AS viagens v1+ v2
SELECT
  p.`data`,
  p.tipo_dia,
  p.consorcio,
  p.servico,
  IFNULL(COALESCE(v1.tipo_viagem, v2.tipo_viagem), "Sem viagem apurada") AS tipo_viagem,
  IFNULL(COALESCE(v1.viagens, v2.viagens), 0) AS viagens,
  IFNULL(COALESCE(v1.km_apurada, v2.km_apurada), 0) AS km_apurada
FROM
  planejado p
LEFT JOIN
  sumario_v1 v1
ON
  p.`data` = v1.`data`
  AND p.servico = v1.servico
LEFT JOIN
  sumario_v2 v2
ON
  p.`data` = v2.`data`
  AND p.servico = v2.servico