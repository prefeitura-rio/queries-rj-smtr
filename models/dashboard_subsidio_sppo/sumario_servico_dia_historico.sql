WITH
  -- v1: Valor DO subsdio pré glosa por tipos de viagem
  sumario_v1 AS ( 
  SELECT
    s.`data`,
    tipo_dia,
    consorcio,
    s.servico,
    vista,
    viagens_subsidio AS viagens,
    distancia_total_subsidio AS km_apurada,
    distancia_total_planejada AS km_planejada,
    perc_distancia_total_subsidio AS perc_km_planejada,
    valor_total_subsidio AS valor_subsidio_pago,
    NULL AS valor_penalidade
  FROM
    {{ ref("sumario_dia") }}  s
    --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_dia` s
  LEFT JOIN (
    SELECT
      DISTINCT DATA,
      servico,
      vista
    FROM
      {{ ref("viagem_planejada") }} 
      --`rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` 
    WHERE
      DATA < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )) p
  USING
    ( DATA,
      servico )
  WHERE
    s.DATA < DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ) ),
  -- v2: Valor DO subsdio pós glosa por tipos de viagem
  sumario_v2 AS (
  SELECT
    s.`data`,
    tipo_dia,
    consorcio,
    s.servico,
    p.vista,
    viagens,
    km_apurada,
    km_planejada,
    perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
  FROM
    {{ ref("sumario_servico_dia") }} s
    --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia` AS s
  LEFT JOIN (
    SELECT
      DISTINCT DATA,
      servico,
      vista
    FROM
      {{ ref("viagem_planejada") }} 
      -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` 
    WHERE
      DATA >= DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ) ) p
  USING
    (DATA,
      servico) ),
  -- v3: Valor DO subsídio sem glosas a partir de 16/07/2023 (2.81/km em 2023)
  sumario_v3 AS ( 
  SELECT
    * EXCEPT (valor_subsidio_pago,
      valor_penalidade),
    v3.valor_subsidio_pago,
    v3.valor_penalidade
  FROM
    sumario_v2 v2
  -- Calcula o valor cheio DO subsidio sem veículos não licenciados
  LEFT JOIN ( 
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
      --`rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_tipo`
    WHERE
      DATA >= DATE( "{{ var("DATA_SUBSIDIO_V3_INICIO") }}" ) ) v3

      
  USING
    ( DATA,
      servico )
  WHERE
    v2.data >= DATE( "{{ var("DATA_SUBSIDIO_V3_INICIO") }}" ) )
SELECT
  *
FROM
  sumario_v1
UNION ALL (
  SELECT
    *
  FROM
    sumario_v2
  WHERE
    DATA < DATE( "{{ var("DATA_SUBSIDIO_V3_INICIO") }}" ) )
UNION ALL (
  SELECT
    data,
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
    sumario_v3 ) 