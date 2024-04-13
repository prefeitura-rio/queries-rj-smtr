{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
  )
}}

WITH gps_sppo AS (
  SELECT
    data,
    SUBSTR(id_veiculo, 2) AS id_veiculo,
    COUNT(DISTINCT timestamp_gps) AS quantidade_gps_sppo
  FROM
    {{ ref("gps_sppo") }}
  WHERE
    DATA = DATE_SUB(DATE('{{ var("run_date") }}'), INTERVAL 1 DAY)
  GROUP BY
    1,
    2 
),
planilha_ronald AS (
  SELECT DISTINCT
    TRIM(numero_ordem_veiculo) AS numero_ordem_veiculo,
    TRIM(numero_serie_validador) AS id_validador,
    empresa
  FROM
    {{ source("dashboard_controle_vinculo_jae_riocard_staging", "relatorio_instalacao_jae") }}
  WHERE
    LENGTH(numero_ordem_veiculo) = 5
    AND numero_ordem_veiculo != '99999'
),
gps_sppo_planilha AS (
  SELECT
    COALESCE(g.data, DATE_SUB(DATE('{{ var("run_date") }}'), INTERVAL 1 DAY)) AS data,
    COALESCE(g.id_veiculo, TRIM(r.numero_ordem_veiculo)) AS id_veiculo,
    COALESCE(g.quantidade_gps_sppo, 0) AS quantidade_gps_sppo,
    TRIM(r.id_validador) AS id_validador,
    r.empresa,
    g.id_veiculo IS NOT NULL AS indicador_veiculo_gps_sppo,
    r.numero_ordem_veiculo IS NOT NULL AS indicador_veiculo_controle_ronald
  FROM
    gps_sppo g
  FULL OUTER JOIN
    planilha_ronald r
  ON
    g.id_veiculo = TRIM(r.numero_ordem_veiculo)
),
sppo_licenciamento AS (
  SELECT DISTINCT
    SUBSTR(id_veiculo, 2) AS id_veiculo
  FROM
    {{ ref("sppo_licenciamento") }}
  WHERE
    data = "2024-03-25"
),
gps_sppo_licenciamento AS (
  SELECT
    COALESCE(g.data, DATE_SUB(DATE('{{ var("run_date") }}'), INTERVAL 1 DAY)) AS data,
    COALESCE(g.id_veiculo, l.id_veiculo) AS id_veiculo,
    COALESCE(g.quantidade_gps_sppo, 0) AS quantidade_gps_sppo,
    g.id_validador,
    g.empresa,
    l.id_veiculo IS NOT NULL AS indicador_veiculo_licenciamento,
    COALESCE(g.indicador_veiculo_gps_sppo, FALSE) AS indicador_veiculo_gps_sppo,
    COALESCE(g.indicador_veiculo_controle_ronald, FALSE) AS indicador_veiculo_controle_ronald
  FROM
    gps_sppo_planilha g
  FULL OUTER JOIN
    sppo_licenciamento l
  ON
    l.id_veiculo = g.id_veiculo
),
gps_validador AS (
  SELECT
    data,
    id_validador,
    COUNT(DISTINCT id_transmissao_gps) AS quantidade_gps_validador
  FROM
    {{ ref("gps_validador") }}
  WHERE
    data = DATE_SUB(DATE('{{ var("run_date") }}'), INTERVAL 1 DAY)
  GROUP BY
    1,
    2
),
transacao_riocard AS (
  SELECT DISTINCT
    data,
    id_transacao,
    id_validador
  FROM
    {{ ref("transacao_riocard") }}
  WHERE
    data = DATE_SUB(DATE('{{ var("run_date") }}'), INTERVAL 1 DAY)
),
transacao_agg AS (
  SELECT
    data,
    id_validador,
    COUNT(*) AS quantidade_transacao
  FROM
    transacao_riocard
  GROUP BY
    1,
    2 
)
SELECT
  gs.data,
  gs.id_veiculo,
  gs.empresa,
  gs.id_validador AS numero_serie_validador,
  gs.indicador_veiculo_licenciamento,
  gs.indicador_veiculo_gps_sppo,
  gs.indicador_veiculo_controle_ronald,
  gv.id_validador IS NOT NULL AS indicador_validador_ativo,
  IFNULL(t.quantidade_transacao, 0) AS quantidade_transacao_riocard,
  IFNULL(quantidade_gps_sppo, 0) AS quantidade_gps_sppo,
  IFNULL(quantidade_gps_validador, 0) AS quantidade_gps_validador,
  SAFE_DIVIDE(quantidade_gps_validador, quantidade_gps_sppo) AS percentual_gps_validador_sppo
FROM
  gps_sppo_licenciamento gs
LEFT JOIN
  gps_validador gv
ON
  gs.id_validador = gv.id_validador
  AND gs.data = gv.data
LEFT JOIN
  transacao_agg t
ON
  gs.id_validador = t.id_validador
  AND gs.data = t.data