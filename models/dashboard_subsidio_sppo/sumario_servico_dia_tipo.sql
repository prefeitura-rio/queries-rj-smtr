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
    DATA BETWEEN DATE("{{ var("DATA_SUBSIDIO_V2_INICIO") }}")
    AND DATE("{{ var("end_date") }}")
    AND distancia_total_planejada > 0 ),
  veiculos AS (
  SELECT
    DATA,
    id_veiculo,
    status
  FROM
    {{ ref("sppo_veiculo_dia") }}
  WHERE
    DATA BETWEEN DATE("{{ var("DATA_SUBSIDIO_V2_INICIO") }}")
    AND DATE("{{ var("end_date") }}")),
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
    DATA BETWEEN DATE("{{ var("DATA_SUBSIDIO_V2_INICIO") }}")
    AND DATE("{{ var("end_date") }}")),
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
  servico_km AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.consorcio,
    p.servico,
    v.tipo_viagem,
    IFNULL(v.viagens, 0) AS viagens,
    IFNULL(v.km_apurada, 0) AS km_apurada,
  FROM
    planejado p
  LEFT JOIN
    servico_km_tipo v
  ON
    p.data = v.data
    AND p.servico = v.servico ),
  pivot_data AS (
  SELECT
    *
  FROM (
    SELECT
      data,
      tipo_dia,
      consorcio,
      servico,
      tipo_viagem,
      viagens,
      km_apurada,
    FROM
      servico_km ) PIVOT(SUM(viagens) AS viagens,
      SUM(km_apurada) AS km_apurada FOR tipo_viagem IN ("Nao licenciado" AS n_licenciado,
        "Licenciado sem ar" AS licenciado_sem_ar,
        "Licenciado com ar e não autuado (023.II)" AS licenciado_ar_n_autuado,
        "Licenciado com ar e autuado (023.II)" AS licenciado_ar_autuado)))
SELECT
  sd.*,
  pd.* EXCEPT(data,
    tipo_dia,
    servico,
    consorcio)
FROM
  {{ ref("sumario_servico_dia") }} AS sd
LEFT JOIN
  pivot_data AS pd
ON
  sd.data = pd.data
  AND sd.servico = pd.servico
ORDER BY
  DATA,
  servico