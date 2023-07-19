WITH
  planejado AS (
  SELECT
    consorcio,
    data,
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
    {{ ref("viagem_planejada") }} --``rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
  WHERE
    data >= "2022-06-01"
    AND data < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND distancia_total_planejada > 0
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
  viagem AS (
  SELECT
    data,
    trip_id,
    COUNT(id_viagem) AS viagens_realizadas
  FROM
    {{ ref("viagem_completa") }} -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
  WHERE
    data >= "2022-06-01"
    AND data < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
  GROUP BY
    1,
    2),
  sumario AS (
  SELECT
    consorcio,
    data,
    tipo_dia,
    servico,
    distancia_planejada,
    NULL AS viagens_planejadas,
    IFNULL(SUM(v.viagens_realizadas), 0) AS viagens_subsidio,
    distancia_total_planejada,
    (IFNULL(SUM(v.viagens_realizadas), 0) * distancia_planejada) AS distancia_total_subsidio
  FROM
    planejado AS p
  LEFT JOIN
    viagem AS v
  USING
    (trip_id,
      data)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    8 ),
  sumario_agg AS (
  SELECT
    consorcio,
    data,
    tipo_dia,
    servico,
    NULL AS viagens_planejadas,
    IFNULL(SUM(viagens_subsidio), 0) AS viagens_subsidio,
    distancia_total_planejada,
    ROUND(SUM(distancia_total_subsidio), 3) AS distancia_total_subsidio
  FROM
    sumario
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    7 ),
  valor AS (
  SELECT
    s.*,
    v.valor_subsidio_por_km,
    ROUND(distancia_total_subsidio * v.valor_subsidio_por_km, 2) AS valor_total_aferido,
  IF
    (distancia_total_planejada = 0, NULL, ROUND(100*distancia_total_subsidio/distancia_total_planejada, 2)) AS perc_distancia_total_subsidio
  FROM
    sumario_agg s
  LEFT JOIN (
    SELECT
      *
    FROM
      {{ ref("subsidio_data_versao_efetiva") }} -- `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
    WHERE
      data >= "2022-06-01"
      AND data < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )) AS v
  ON
    v.data = s.data )
SELECT
  *,
  CASE
    WHEN (perc_distancia_total_subsidio < 80) OR (perc_distancia_total_subsidio IS NULL) THEN 0
  ELSE
  valor_total_aferido
END
  AS valor_total_subsidio
FROM
  valor