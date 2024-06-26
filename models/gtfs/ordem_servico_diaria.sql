{{ config(
  materialized="view"
) }} 

WITH
  feed_start_date AS (
  SELECT
    feed_start_date,
    feed_start_date AS data_inicio,
    COALESCE(DATE_SUB(LEAD(feed_start_date) OVER (ORDER BY feed_start_date), INTERVAL 1 DAY), LAST_DAY(feed_start_date, MONTH)) AS data_fim
  FROM (
    SELECT
      DISTINCT feed_start_date,
    FROM
      {{ ref("ordem_servico_gtfs") }} )),
  ordem_servico_pivot AS (
  SELECT
    *
  FROM
    {{ ref("ordem_servico_gtfs") }} 
    PIVOT( MAX(partidas_ida) AS partidas_ida,
      MAX(partidas_volta) AS partidas_volta,
      MAX(viagens_planejadas) AS viagens_planejadas,
      MAX(distancia_total_planejada) AS km FOR 
      tipo_dia IN ( 
        'Dia Útil' AS du,
        'Ponto Facultativo' AS pf,
        'Sabado' AS sab,
        'Domingo' AS dom ))),
  subsidio_feed_start_date_efetiva AS (
  SELECT
    data,
    SPLIT(tipo_dia, " - ")[0] AS tipo_dia,
    tipo_dia AS tipo_dia_original
  FROM
    {{ ref("subsidio_data_versao_efetiva") }} )
SELECT
  DATA,
  tipo_dia_original AS tipo_dia,
  servico,
  vista,
  consorcio,
  sentido,
  CASE
  {% set tipo_dia = {"Dia Útil": "du", "Ponto Facultativo": "pf", "Sabado": "sab", "Domingo": "dom"} %}
  {% set sentido = {"ida": ("I", "C"), "volta": "V"} %}
  {%- for key_s, value_s in sentido.items() %}
    {%- for key_td, value_td in tipo_dia.items() %}
      WHEN sentido {% if key_s == "ida" %} IN {{ value_s }} {% else %} = "{{ value_s }}" {% endif %} AND tipo_dia = "{{ key_td }}" THEN {% if key_td in ["Sabado", "Domingo"] %} ROUND(SAFE_DIVIDE((partidas_{{ key_s }}_du * km_{{ value_td }}), km_du)) {% else %} partidas_{{ key_s }}_{{ value_td }} {% endif %}
    {% endfor -%}
  {% endfor -%}
END
  AS viagens_planejadas,
  horario_inicio AS inicio_periodo,
  horario_fim AS fim_periodo
FROM
  UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(data_inicio) FROM feed_start_date), (SELECT MAX(data_fim) FROM feed_start_date))) AS DATA
LEFT JOIN
  feed_start_date AS d
ON
  DATA BETWEEN d.data_inicio
  AND d.data_fim
LEFT JOIN
  subsidio_feed_start_date_efetiva AS sd
USING
  (DATA)
LEFT JOIN
  ordem_servico_pivot AS o
USING
  (feed_start_date)
LEFT JOIN
  {{ ref("servicos_sentido") }}
USING
  (feed_start_date,
    servico)
