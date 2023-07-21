-- Gabarito v1 + v2
WITH
  planejado AS (
  SELECT
    DISTINCT `data`,
    tipo_dia,
    consorcio,
    servico
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    `data` <= DATE( "{{ var("end_date") }}" )
    AND distancia_total_planejada is not null ),
  sumario_v1 AS ( -- Viagens v1
  SELECT
    `data`,
    servico,
    "Não classificado" AS tipo_viagem,
    NULL AS indicador_ar_condicionado,
    viagens,
    ROUND(km_apurada, 2) AS km_apurada
  FROM
     {{ ref("sumario_servico_dia_historico") }}
  WHERE
    `data` < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ) ),
  tipo_viagem_v2 AS ( -- Classifica os tipos de viagem (v2)
  SELECT
    `data`,
    id_veiculo,
    status,
    SAFE_CAST(JSON_VALUE(indicadores,"$.indicador_ar_condicionado") AS BOOL) AS indicador_ar_condicionado
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
    {{ ref("viagem_completa") }}
  WHERE
    `data` BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE( "{{ var("end_date") }}" ) ),
  subsidio_parametros AS (
  SELECT
    *
  FROM
    {{ ref("subsidio_parametros") }}
  WHERE
    status != "Não classificado"
  ORDER BY
    data_inicio DESC,
    ordem),
  tabela_status_array AS (
  SELECT
    TO_JSON_STRING(STRUCT(indicador_licenciado,
        indicador_ar_condicionado,
        indicador_autuacao_ar_condicionado,
        indicador_autuacao_seguranca,
        indicador_autuacao_limpeza,
        indicador_autuacao_equipamento,
        indicador_sensor_temperatura,
        indicador_validador_sbd )) AS indicadores,
    ARRAY_AGG(status) AS status_array
  FROM
    subsidio_parametros
  GROUP BY
    indicadores),
  status_update AS (
  SELECT
    indicadores,
    status_array,
    status_array[OFFSET(0)] AS status
  FROM
    tabela_status_array),
  tipo_viagem_v2_atualizado AS (
  SELECT
    * EXCEPT(status),
    u.status
  FROM
    tipo_viagem_v2 AS k
  LEFT JOIN
    status_update AS u
  ON
    k.status IN UNNEST(u.status_array)),
  sumario_v2 AS (
  SELECT
    v.`data`,
    v.servico,
    ve.status AS tipo_viagem,
    ve.indicador_ar_condicionado,
    COUNT(id_viagem) AS viagens,
    ROUND(SUM(distancia_planejada), 2) AS km_apurada
  FROM
    viagem_v2 v
  LEFT JOIN
    tipo_viagem_v2_atualizado ve
  ON
    ve.`data` = v.`data`
    AND ve.id_veiculo = v.id_veiculo
  GROUP BY
    1,
    2,
    3,
    4 ) -- Todas AS viagens v1+ v2
(SELECT
  p.`data`,
  p.tipo_dia,
  p.consorcio,
  p.servico,
  IFNULL(v1.tipo_viagem, "Sem viagem apurada") AS tipo_viagem,
  SAFE_CAST(indicador_ar_condicionado AS BOOL) AS indicador_ar_condicionado,
  IFNULL(v1.viagens, 0) AS viagens,
  IFNULL(v1.km_apurada, 0) AS km_apurada
FROM
  planejado p
LEFT JOIN
  sumario_v1 v1
ON
  p.`data` = v1.`data`
  AND p.servico = v1.servico)
UNION ALL
(SELECT
  p.`data`,
  p.tipo_dia,
  p.consorcio,
  p.servico,
  IFNULL(v2.tipo_viagem, "Não identificada") AS tipo_viagem,
  v2.indicador_ar_condicionado,
  IFNULL(v2.viagens, 0) AS viagens,
  IFNULL(v2.km_apurada, 0) AS km_apurada
FROM
  planejado p
LEFT JOIN
  sumario_v2 v2
ON
  p.`data` = v2.`data`
  AND p.servico = v2.servico)