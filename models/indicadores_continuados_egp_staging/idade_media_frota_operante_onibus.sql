{{
  config( 
    partition_by = { 
    "field": "data",
    "data_type": "date",
    "granularity": "month"
    },
)}}

WITH
  -- 1. Seleciona a última data disponível de cada mês
  datas AS (
  SELECT
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(YEAR FROM data) AS ano,
    MAX(data) AS data
  FROM
    {{ ref("sppo_licenciamento") }}
    --rj-smtr.veiculo.sppo_licenciamento
  WHERE
  {% if is_incremental() %}
    data BETWEEN DATE_TRUNC(DATE("{{ var("start_date") }}"), MONTH)
    AND LAST_DAY(DATE("{{ var("end_date") }}"), MONTH)
    AND data < DATE_TRUNC(CURRENT_DATE(), MONTH)
  {% else %}
    data < DATE_TRUNC(CURRENT_DATE(), MONTH)
  {% endif %}
  GROUP BY
    1,
    2),
  -- 2. Verifica frota operante
  frota_operante AS (
  SELECT
    DISTINCT id_veiculo,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(YEAR FROM data) AS ano,
  FROM
    {{ ref('viagem_completa') }}
    --rj-smtr.projeto_subsidio_sppo.viagem_completa
  WHERE
  {% if is_incremental() %}
    data BETWEEN DATE_TRUNC(DATE("{{ var("start_date") }}"), MONTH)
    AND LAST_DAY(DATE("{{ var("end_date") }}"), MONTH)
    AND data < DATE_TRUNC(CURRENT_DATE(), MONTH)
  {% else %}
    data < DATE_TRUNC(CURRENT_DATE(), MONTH)
  {% endif %}
  ),
  -- 3. Calcula a idade de todos os veículos para a data de referência
  idade_frota AS (
  SELECT
    data,
    EXTRACT(YEAR FROM data) - CAST(ano_fabricacao AS INT64) AS idade
  FROM
    datas AS d
  LEFT JOIN
    {{ ref("sppo_licenciamento") }}
    --rj-smtr.veiculo.sppo_licenciamento AS l
  USING
    (data)
  LEFT JOIN
    frota_operante AS f
  USING
    (id_veiculo, mes, ano)
  WHERE
    f.id_veiculo IS NOT NULL
  )
-- 4. Calcula a idade média
SELECT
  data,
  EXTRACT(YEAR FROM data) AS ano,
  EXTRACT(MONTH FROM data) AS mes,
  "Ônibus" AS modo,
  ROUND(AVG(idade),2) AS idade_media_veiculo_mes,
  CURRENT_DATE() AS data_ultima_atualizacao,
  '{{ var("version") }}' as versao
FROM
  idade_frota
GROUP BY
  1,
  2,
  3
