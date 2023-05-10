SELECT
  s.`data`,
  tipo_dia,
  consorcio,
  s.servico,
  vista,
  viagens,
  km_apurada,
  km_planejada,
  perc_km_planejada,
  valor_subsidio_pago,
  valor_penalidade
FROM
  {{ ref("sumario_servico_dia") }} s
LEFT JOIN (
  SELECT
    DISTINCT data,
    servico,
    vista
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    data >= "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" ) p
ON
  s.data = p.data
  AND s.servico = p.servico
UNION ALL (
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
    {{ ref("sumario_dia") }} s
  LEFT JOIN (
    SELECT
      DISTINCT data,
      servico,
      vista
    FROM
      {{ ref("viagem_planejada") }}
    WHERE
      data < "{{ var("DATA_SUBSIDIO_V2_INICIO") }}") p
  ON
    s.data = p.data
    AND s.servico = p.servico
  WHERE
    s.data < "{{ var("DATA_SUBSIDIO_V2_INICIO") }}")