{{ config(
  materialized="view"
) }} 

WITH
  -- TODO: (1) Usar redis para controle de data_versao e passar via parâmetro (otimizar recursos e não precisar percorrer a tabela inteira)
  --       (2) Criar tabela a parte para controle de data_versao e materializar com o parâmetro data_versao_gtfs
  data_versao AS (
  SELECT
    data_versao,
    data_versao AS data_inicio,
    COALESCE(DATE_SUB(LEAD(data_versao) OVER (ORDER BY data_versao), INTERVAL 1 DAY), LAST_DAY(data_versao, MONTH)) AS data_fim
  FROM (
    SELECT
      DISTINCT data_versao,
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
      -- TODO: (3) Usar Jinja para gerar lista independentemente da quantidade de tipo_dia
        'Dia Útil' AS du,
        'Ponto Facultativo' AS pf,
        'Sabado' AS sab,
        'Domingo' AS dom )))
SELECT
  DATA,
  tipo_dia,
  servico,
  vista,
  consorcio,
  sentido,
  CASE
  -- TODO: Considerar (3)
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
  -- TODO: (4) É possível otimizar?
  UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(data_inicio) FROM data_versao), (SELECT MAX(data_fim) FROM data_versao))) AS DATA
LEFT JOIN
  data_versao AS d
ON
  DATA BETWEEN d.data_inicio
  AND d.data_fim
LEFT JOIN
  -- TODO: (5) Trocar referência para calendar_dates
  {{ ref("subsidio_data_versao_efetiva") }} AS sd
USING
  (DATA)
LEFT JOIN
  ordem_servico_pivot AS o
USING
  (data_versao)
LEFT JOIN
  {{ ref("servicos_sentido") }}
USING
  (data_versao,
    servico)
