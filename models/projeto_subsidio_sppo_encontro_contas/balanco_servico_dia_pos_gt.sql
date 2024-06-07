{% if var("encontro_contas_modo") == "_pos_gt" %}
-- 0. Lista servicos e dias atípicos (pagos por recurso)
WITH
  recursos AS (
  SELECT
    data,
    id_recurso,
    tipo_recurso,
    -- consorcio,
    servico,
    SUM(valor_pago) AS valor_pago
  FROM
    {{ ref("recursos_sppo_servico_dia_pago") }}
    -- `rj-smtr`.`br_rj_riodejaneiro_recursos`.`recursos_sppo_servico_dia_pago`
  GROUP BY
    1,
    2,
    3,
    4),
servico_dia_atipico as (
SELECT
  DISTINCT data, servico
FROM
  recursos
WHERE
  -- Quando o valor do recurso pago for R$ 0, desconsidera-se o recurso, pois:
    -- Recurso pode ter sido cancelado (pago e depois revertido)
    -- Problema reporto não gerou impacto na operação (quando aparece apenas 1 vez)
  valor_pago != 0
  -- Desconsideram-se recursos do tipo "Algoritmo" (igual a apuração em produção, levantado pela TR/SUBTT/CMO) 
  -- Desconsideram-se recursos do tipo "Viagem Individual" (não afeta serviço-dia)
  AND tipo_recurso NOT IN ("Algoritmo", "Viagem Individual")
  -- Desconsideram-se recursos de reprocessamento que já constam em produção
  AND NOT (data BETWEEN "2022-06-01" AND "2022-06-30" 
            AND tipo_recurso = "Reprocessamento")
),

-- 1. Calcula a km subsidiada por servico e dia
sumario_dia AS (  -- Km apurada por servico e dia
  SELECT
    data,
    consorcio,
    servico,
    SUM(km_apurada) AS km_subsidiada,
    sum(valor_subsidio_pago) as subsidio_pago
  FROM
    {{ ref("sumario_servico_dia_historico") }}
    -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
  WHERE
    data BETWEEN "2022-06-01"
    AND "2023-12-31"
    and valor_subsidio_pago > 0
  GROUP BY
    1,
    2,
    3),
  viagem_remunerada AS ( -- Km subsidiada pos regra do teto de 120% por servico e dia
  SELECT
    data,
    servico,
    SUM(distancia_planejada) AS km_subsidiada
  FROM
    {{ ref("viagens_remuneradas") }}
    -- `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
  WHERE
    data BETWEEN "2023-09-16"
    AND "2023-12-31"
    AND indicador_viagem_remunerada = TRUE -- useless
  GROUP BY
    1,
    2 ),
km_subsidiada_dia as (
  SELECT
  sd.* except(km_subsidiada),
    ifnull(case when data >= "2023-09-16" then vr.km_subsidiada else sd.km_subsidiada end, 0) as km_subsidiada
  FROM
    sumario_dia sd
  LEFT JOIN
    viagem_remunerada as vr
  using
    (data, servico)
),

-- 2. Filtra km subsidiada apenas em dias típicos (remove servicos e dias pagos por recurso)
km_subsidiada_filtrada as (
  select
    ksd.*
  from km_subsidiada_dia ksd
  left join servico_dia_atipico sda
  using (data, servico)
  where sda.data is null
  -- Demais dias que não foi considerada a km apurada via GPS:
  and ksd.data NOT IN ("2022-10-02", "2022-10-30", '2023-02-07', '2023-02-08', '2023-02-10', '2023-02-13', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')
),

-- 3. Calcula a receita tarifária por servico e dia
rdo AS (
  SELECT
    data,
    consorcio,
    CASE
      WHEN LENGTH(linha) < 3 THEN LPAD(linha, 3, "0")
    ELSE
    CONCAT( IFNULL(REGEXP_EXTRACT(linha, r"[A-Z]+"), ""), IFNULL(REGEXP_EXTRACT(linha, r"[0-9]+"), "") )
  END
    AS servico,
    SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie) AS receita_tarifaria_aferida
  FROM
    {{ ref("rdo40_registros") }}
    -- `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_registros`
  WHERE
    data BETWEEN "2022-06-01" AND "2023-12-31"
    AND data NOT IN ("2022-10-02", "2022-10-30", '2023-02-07', '2023-02-08', '2023-02-10', '2023-02-13', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')
    and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
  group by 1,2,3
),

-- 3.1. Lista os serviços conforme tratamento indicado em resposta aos ofícios MTR-OFI-2024/03024, MTR-OFI-2024/03025, MTR-OFI-2024/03026 e MTR-OFI-2024/03027 (Processo MTR-PRO-2024/06270)
rdo_correcao_servico AS (
  SELECT DISTINCT
    data_inicio_quinzena, 
    data_final_quinzena, 
    servico_tratado_rdo, 
    servico_corrigido_rioonibus
  FROM
    {{ ref("rdo_correcao_rioonibus_servico_quinzena") }}
),

-- 3.2. Adiciona serviços corrigidos do RDO
rdo_servico_corrigido AS (
  SELECT
    data,
    consorcio,
    rdo.servico,
    cro.servico_corrigido_rioonibus, 
    receita_tarifaria_aferida
  FROM
    rdo
  LEFT JOIN
    rdo_correcao_servico AS cro
  ON
    rdo.data BETWEEN cro.data_inicio_quinzena AND cro.data_final_quinzena
    AND rdo.servico = cro.servico_tratado_rdo
),

-- 3.3. Corrige serviços do RDO com base nos serviços subsidiados (ou seja, apenas serviços planejados no dia)
rdo_corrigido AS (
  SELECT
    rdo.data,
    ksf.consorcio,
    ksf.servico,
    SUM(receita_tarifaria_aferida) AS receita_tarifaria_aferida
  FROM
    rdo_servico_corrigido AS rdo
  LEFT JOIN
    km_subsidiada_filtrada AS ksf
  ON
    rdo.data = ksf.data
    AND 
      (rdo.servico_corrigido_rioonibus = ksf.servico
      OR rdo.servico = ksf.servico)
  GROUP BY
    1,
    2,
    3
),

-- 4. Calcula valores esperados de receita e subsídio com base nos parâmetros de remuneração por km
parametros as (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    irk,
    case 
      when data_fim <= "2022-12-31" then irk - subsidio_km  -- subsidio varia ao longo dos meses
      else coalesce(irk_tarifa_publica, irk - (subsidio_km + desconto_subsidio_km)) end as irk_tarifa_publica,
    (subsidio_km + desconto_subsidio_km) as subsidio_km
  FROM
    {{ source("projeto_subsidio_sppo_encontro_contas", "parametros_km") }}
  where data_inicio >= "2022-06-01" and data_fim <= "2023-12-31"
)
  select
    *,
    ifnull(receita_total_aferida, 0) - ifnull(receita_total_esperada - subsidio_glosado, 0) as saldo
  from (
    select
      ks.* except(subsidio_pago),
      ks.km_subsidiada * par.irk as receita_total_esperada,
      ks.km_subsidiada * par.irk_tarifa_publica as receita_tarifaria_esperada,
      ks.km_subsidiada * par.subsidio_km as subsidio_esperado,
      case when data >= "2023-01-01" then (ks.km_subsidiada * par.subsidio_km - subsidio_pago) else 0 end as subsidio_glosado,
      ifnull(rdo.receita_tarifaria_aferida, 0) + ifnull(ks.subsidio_pago, 0) as receita_total_aferida,
      rdo.receita_tarifaria_aferida,
      ks.subsidio_pago
    from
      km_subsidiada_filtrada ks
    left join
      rdo_corrigido AS rdo
    using 
      (data, servico, consorcio)
    left join
      parametros par
    on
      ks.data between data_inicio and data_fim
  )
{% else %}
{{ config(enabled=false) }}
{% endif %}