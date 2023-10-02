WITH
  viagem_planejada AS (
  SELECT
    DISTINCT `data`,
    servico,
    vista
  FROM
    {{ ref("viagem_planejada") }}
    --`rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` 
  ),
  -- v1: Valor do subsídio pré glosa por tipos de viagem (Antes de 2023-01-16)
  sumario_sem_glosa AS (
  SELECT
    `data`,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens_subsidio AS viagens,
    distancia_total_subsidio AS km_apurada,
    distancia_total_planejada AS km_planejada,
    perc_distancia_total_subsidio AS perc_km_planejada,
    valor_total_subsidio AS valor_subsidio_pago,
    NULL AS valor_penalidade
  FROM
    {{ ref("sumario_dia") }}
    -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_dia`
  LEFT JOIN
    viagem_planejada
  USING
    ( `data`,
      servico ) ),
  -- v2: Valor do subsídio pós glosa por tipos de viagem (2023-01-16 a 2023-07-15 e após de 2023-09-01)
  sumario_com_glosa AS (
  SELECT
    `data`,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
  FROM
    {{ ref("sumario_servico_dia") }}
    -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia`
  LEFT JOIN
    viagem_planejada
  USING
    ( `data`,
      servico )),
  -- Valor do subsídio sem glosas - Suspenso por Decisão Judicial (Entre 2023-07-16 e 2023-08-31) (R$ 2.81/km em 2023)
  subsidio_total_glosa_suspensa AS (
  SELECT
    DATA,
    servico,
    CASE
      WHEN perc_km_planejada >= 80 THEN ROUND((COALESCE(km_apurada_autuado_ar_inoperante, 0) + COALESCE(km_apurada_autuado_seguranca, 0) + COALESCE(km_apurada_autuado_limpezaequipamento, 0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado, 0) + COALESCE(km_apurada_licenciado_com_ar_n_autuado, 0)) * 2.81, 2)
    ELSE
    0
  END
    AS valor_subsidio_pago,
    0 AS valor_penalidade
  FROM
    {{ ref("sumario_servico_dia_tipo") }}
    --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo`
  WHERE
    DATA BETWEEN "2023-07-16"
    AND "2023-08-31"),
  -- v3: Sumário subsídio sem glosas - Suspenso por Decisão Judicial (Entre 2023-07-16 e 2023-08-31)
  sumario_glosa_suspensa AS (
  SELECT
    s.* EXCEPT (valor_subsidio_pago,
      valor_penalidade),
    g.valor_subsidio_pago,
    g.valor_penalidade
  FROM
    subsidio_total_glosa_suspensa AS g
  LEFT JOIN
    sumario_com_glosa AS s
  USING
    ( `data`,
      servico ))
SELECT
  *
FROM
  sumario_sem_glosa
UNION ALL (
  SELECT
    *
  FROM
    sumario_com_glosa
  WHERE
    `data` < "2023-07-16"
    OR `data` > "2023-08-31" )
UNION ALL (
  SELECT
    *
  FROM
    sumario_glosa_suspensa )