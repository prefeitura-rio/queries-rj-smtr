{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data", 
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

WITH licenciamento AS (
  SELECT
    data,
    REGEXP_REPLACE(id_veiculo, r'[^0-9]', '') AS id_veiculo,
    status
  FROM
    -- {{ ref("sppo_veiculo_dia") }} l
    rj-smtr.veiculo.sppo_veiculo_dia
  WHERE
    {% if is_incremental() %}
      data BETWEEN DATE_SUB("{{var('date_range_start')}}", INTERVAL 7 DAY) AND DATE_SUB("{{var('date_range_end')}}", INTERVAL 7 DAY)
    {% else %}
      data >= "2024-04-01"
    {% endif %}
),
gps_sppo AS (
  SELECT
    data,
    REGEXP_REPLACE(id_veiculo, r'[^0-9]', '') AS id_veiculo,
    COUNT(DISTINCT CONCAT(id_veiculo, timestamp_gps, servico)) AS quantidade_gps_onibus
  FROM
    -- {{ ref("gps_sppo") }}
    rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo
  WHERE
    {% if is_incremental() %}
      data BETWEEN DATE_SUB("{{var('date_range_start')}}", INTERVAL 7 DAY) AND DATE_SUB("{{var('date_range_end')}}", INTERVAL 7 DAY)
    {% else %}
      data BETWEEN DATE("2024-04-01") AND DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 7 DAY)
    {% endif %}
  GROUP BY
    1,
    2
),
transacao AS (
  SELECT
    data,
    id_veiculo,
    id_validador,
    COUNT(DISTINCT id_transacao) AS quantidade_transacao
  FROM
    {{ ref("transacao") }}
  WHERE
    {% if is_incremental() %}
      data BETWEEN DATE_SUB("{{var('date_range_start')}}", INTERVAL 5 DAY) AND DATE_SUB("{{var('date_range_end')}}", INTERVAL 7 DAY)
    {% else %}
      data BETWEEN DATE("2024-04-01") AND DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 7 DAY)
    {% endif %}
  GROUP BY
    1,
    2,
    3
),
gps_validador AS (
  SELECT
    data,
    id_veiculo,
    id_validador,
    COUNT(DISTINCT id_transmissao_gps) AS quantidade_gps_validador
  FROM
    {{ ref("gps_validador") }}
  WHERE
    {% if is_incremental() %}
      data BETWEEN DATE_SUB("{{var('date_range_start')}}", INTERVAL 7 DAY) AND DATE_SUB("{{var('date_range_end')}}", INTERVAL 7 DAY)
    {% else %}
      data BETWEEN DATE("2024-04-01") AND DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 7 DAY)
    {% endif %}
    AND modo = "Ônibus"
  GROUP BY
    1,
    2,
    3
)
SELECT
  gv.data,
  gv.id_validador,
  gv.id_veiculo,
  gv.quantidade_gps_validador,
  IFNULL(g.quantidade_gps_onibus, 0) AS quantidade_gps_onibus,
  IFNULL(t.quantidade_transacao, 0) AS quantidade_transacao,
  l.id_veiculo IS NOT NULL AND l.status NOT IN ('Nao licenciado', 'Não licenciado') AS indicador_veiculo_licenciado
FROM
  gps_validador gv
LEFT JOIN
  gps_sppo g
ON
  g.id_veiculo = gv.id_veiculo
  AND g.data = gv.data
LEFT JOIN
  transacao t
ON
  t.id_veiculo = gv.id_veiculo
  AND t.id_validador = gv.id_validador
  AND t.data = gv.data
LEFT JOIN
  licenciamento l
ON
  l.id_veiculo = gv.id_veiculo
  AND l.data = gv.data