{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}

WITH
-- 1. Viagens planejadas (agrupadas por data e serviço)
  planejado AS (
  SELECT
    DISTINCT data,
    tipo_dia,
    consorcio,
    servico,
    distancia_total_planejada AS km_planejada,
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
    AND ( distancia_total_planejada > 0
      OR distancia_total_planejada IS NULL )
  ),
  viagens_planejadas AS (
  SELECT
    data_versao,
    servico,
    tipo_dia,
    viagens_planejadas,
    partidas_ida,
    partidas_volta
  FROM
      {{ ref("ordem_servico_gtfs") }}
  WHERE
    data_versao BETWEEN DATE_TRUNC(DATE("{{ var("start_date") }}"), MONTH)
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
    viagens_planejadas,
    v.partidas_ida + v.partidas_volta AS viagens_planejadas_ida_volta
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
    AND p.tipo_dia = v.tipo_dia
    AND p.servico = v.servico
  ),
-- 2. Viagens realizadas
  viagem AS (
  SELECT
    data,
    servico_realizado AS servico,
    id_veiculo,
    id_viagem,
    distancia_planejada
 FROM
    {{ ref("viagem_completa") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" ) ),
-- 3. Status dos veículos
  veiculos AS (
  SELECT
    data,
    id_veiculo,
    status
  FROM
    {{ ref("sppo_veiculo_dia") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}") ),
-- 4. Parâmetros de subsídio
  subsidio_parametros AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    status,
    subsidio_km
  FROM
    {{ ref("subsidio_parametros") }} ),
-- 5. Viagens com tipo e valor de subsídio por km
  viagem_km_tipo AS (
  SELECT
    v.data,
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
    (data,
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
    (data,
      servico)
  GROUP BY
    1,
    2,
    3,
    4,
    5 )
-- 7. Flag de viagens que serão consideradas ou não para fins de remuneração (apuração de valor de subsídio) - RESOLUÇÃO SMTR Nº 3645/2023
SELECT
v.* EXCEPT(rn),
CASE
    WHEN data >= "2023-09-16"
        AND p.tipo_dia = "Dia Útil"
        AND viagens_planejadas > 10
        AND perc_km_planejada > 120
        AND rn > viagens_planejadas_ida_volta*1.2
        THEN FALSE
    WHEN data >= "2023-09-16"
        AND p.tipo_dia = "Dia Útil"
        AND viagens_planejadas <= 10
        AND perc_km_planejada > 200
        AND rn > viagens_planejadas_ida_volta*2
        THEN FALSE
    ELSE
        TRUE
    END AS indicador_viagem_remunerada
FROM (
SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY data, servico ORDER BY subsidio_km*distancia_planejada DESC) AS rn
FROM
    viagem_km_tipo ) AS v
LEFT JOIN
    viagem_planejada AS p
USING
    (data,
        servico)
LEFT JOIN
    servico_km_apuracao AS s
USING
    (data,
        servico)