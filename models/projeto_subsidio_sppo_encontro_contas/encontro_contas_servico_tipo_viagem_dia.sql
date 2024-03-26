{{
  config(
    materialized="ephemeral"
  )
}}

-- Cópia do modelo `sumario_servico_tipo_viagem_dia`. Entretanto, considera apenas as viagens remuneradas (descontadas viagens acima do teto de 120%/200% - RESOLUÇÃO SMTR Nº 3645/2023)

{% set data_fim = "2023-12-31" %}

WITH
  planejado AS (
  SELECT
    DISTINCT `data`,
    tipo_dia,
    consorcio,
    servico
  FROM
    {{ ref("sumario_servico_dia_historico") }}
    --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
  WHERE
    `data` <= DATE("{{ data_fim }}")),
  sumario_v1 AS ( -- Viagens v1
  SELECT
    `data`,
    servico,
    "Não classificado" AS tipo_viagem,
    NULL AS indicador_ar_condicionado,
    viagens,
    km_apurada
  FROM
    {{ ref("sumario_servico_dia_historico") }}
    --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
  WHERE
    `data` < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ) ),
  tipo_viagem_v2 AS ( -- Classifica os tipos de viagem (v2)
  SELECT
    `data`,
    id_veiculo,
    status,
    SAFE_CAST(JSON_VALUE(indicadores,"$.indicador_ar_condicionado") AS BOOL) AS indicador_ar_condicionado
  FROM
    --`rj-smtr`.`veiculo`.`sppo_veiculo_dia`
    {{ ref("sppo_veiculo_dia") }}
  WHERE
    `data` BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE("{{ data_fim }}") ),
  viagem_v2 AS (
  SELECT
    `data`,
    servico_realizado AS servico,
    id_veiculo,
    id_viagem,
    distancia_planejada
  FROM
    --`rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
    {{ ref("viagem_completa") }}
  WHERE
    `data` BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE("{{ data_fim }}") ),
  subsidio_parametros AS (
  SELECT
    *
  FROM
    --`rj-smtr`.`dashboard_subsidio_sppo`.`subsidio_parametros`
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
        indicador_validador_sbd,
        indicador_registro_agente_verao_ar_condicionado )) AS indicadores,
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
  status_flat AS (
  SELECT
    DISTINCT status_t,
    status
  FROM
    status_update,
    UNNEST(status_array) AS status_t),
  tipo_viagem_v2_atualizado AS (
  SELECT
    * EXCEPT(status),
    u.status
  FROM
    tipo_viagem_v2 AS k
  LEFT JOIN
    status_flat AS u
  ON
    u.status_t = k.status),
  sumario_v2 AS (
  SELECT
    v.`data`,
    v.servico,
    ve.status AS tipo_viagem,
    ve.indicador_ar_condicionado,
    COUNT(id_viagem) AS viagens,
    ROUND(SUM(v.distancia_planejada), 2) AS km_apurada
  FROM
    viagem_v2 v
  LEFT JOIN
    tipo_viagem_v2_atualizado ve
  USING
    (data, id_veiculo)
  LEFT JOIN
    {{ ref("viagens_remuneradas") }}
    --`rj-smtr`.`dashboard_subsidio_sppo`.`viagens_remuneradas`
  USING
    (data, servico, id_viagem)
  WHERE
    indicador_viagem_remunerada IS NULL 
    OR indicador_viagem_remunerada IS TRUE
  GROUP BY
    1,
    2,
    3,
    4 )
(
SELECT
  v1.`data`,
  p.tipo_dia,
  p.consorcio,
  v1.servico,
  COALESCE(v1.tipo_viagem, "Sem viagem apurada") AS tipo_viagem,
  SAFE_CAST(indicador_ar_condicionado AS BOOL) AS indicador_ar_condicionado,
  COALESCE(v1.viagens, 0) AS viagens,
  COALESCE(v1.km_apurada, 0) AS km_apurada
FROM
  sumario_v1 v1
INNER JOIN
  planejado p
ON
  p.`data` = v1.`data`
  AND p.servico = v1.servico
WHERE
  p.`data` < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ))
UNION ALL (
SELECT
  v2.`data`,
  p.tipo_dia,
  p.consorcio,
  v2.servico,
  COALESCE(v2.tipo_viagem, "Sem viagem apurada") AS tipo_viagem,
  v2.indicador_ar_condicionado,
  COALESCE(v2.viagens, 0) AS viagens,
  COALESCE(v2.km_apurada, 0) AS km_apurada
FROM
  sumario_v2 v2
INNER JOIN
  planejado p
ON
  p.`data` = v2.`data`
  AND p.servico = v2.servico
WHERE
  p.`data` >= DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ))