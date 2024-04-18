{{ 
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    incremental_strategy="insert_overwrite"
  )
}}

{% if var("run_date") <= var("DATA_SUBSIDIO_V6_INICIO") %}
{% if execute %}
  {% set trips_date = run_query("SELECT MAX(data_versao) FROM " ~ ref("subsidio_trips_desaninhada") ~ " WHERE data_versao >= DATE_TRUNC(DATE_SUB(DATE('" ~ var("run_date") ~ "'), INTERVAL 30 DAY), MONTH)").columns[0].values()[0] %}
  {% set shapes_date = run_query("SELECT MAX(data_versao) FROM " ~ var("subsidio_shapes") ~ " WHERE data_versao >= DATE_TRUNC(DATE_SUB(DATE('" ~ var("run_date") ~ "'), INTERVAL 30 DAY), MONTH)").columns[0].values()[0] %}
  {% set frequencies_date = run_query("SELECT MAX(data_versao) FROM " ~ ref("subsidio_quadro_horario") ~ " WHERE data_versao >= DATE_TRUNC(DATE_SUB(DATE('" ~ var("run_date") ~ "'), INTERVAL 30 DAY), MONTH)").columns[0].values()[0] %}
{% endif %}

WITH
  dates AS (
  SELECT 
    data,
    CASE
        WHEN data = "2022-06-16" THEN "Domingo"
        WHEN data = "2022-06-17" THEN "Sabado"
        WHEN data = "2022-09-02" THEN "Sabado"
        WHEN data = "2022-09-07" THEN "Domingo"
        WHEN data = "2022-10-12" THEN "Domingo"
        WHEN data = "2022-10-17" THEN "Sabado"
        WHEN data = "2022-11-02" THEN "Domingo"
        WHEN data = "2022-11-14" THEN "Sabado"
        WHEN data = "2022-11-15" THEN "Domingo"
        WHEN data = "2022-11-24" THEN "Sabado"
        WHEN data = "2022-11-28" THEN "Sabado"
        WHEN data = "2022-12-02" THEN "Sabado"
        WHEN data = "2022-12-05" THEN "Sabado"
        WHEN data = "2022-12-09" THEN "Sabado"
        WHEN data = "2023-04-06" THEN "Sabado" -- Ponto Facultativo - DECRETO RIO Nº 52275/2023
        WHEN data = "2023-04-07" THEN "Domingo" -- Paixão de Cristo -- Art. 1º, V - PORTARIA ME Nº 11.090/2022
        WHEN data = "2023-06-08" THEN "Domingo" -- Corpus Christi - Lei nº 336/1949 - OFÍCIO Nº MTR-OFI-2023/03260 (MTROFI202303260A)
        WHEN data = "2023-06-09" THEN "Sabado" -- Ponto Facultativo - DECRETO RIO Nº 52584/2023
        WHEN data = "2023-09-08" THEN "Ponto Facultativo" -- Ponto Facultativo - DECRETO RIO Nº 53137/2023
        WHEN data = "2023-10-13" THEN "Ponto Facultativo" -- Ponto Facultativo - DECRETO RIO Nº 53296/2023
        WHEN data = "2023-10-16" THEN "Ponto Facultativo" -- Dia do Comércio - OS Outubro/Q2
        WHEN data = "2023-11-03" THEN "Ponto Facultativo" -- Ponto Facultativo - DECRETO RIO Nº 53417/2023
        WHEN data = "2023-11-05" THEN "Sabado" -- Domingo Atípico - ENEM - OS Novembro/Q1
        WHEN data = "2023-11-12" THEN "Sabado" -- Domingo Atípico - ENEM - OS Novembro/Q1
        WHEN data = "2023-12-02" THEN "Sabado - Verão" -- OS de Verão
        WHEN data = "2023-12-03" THEN "Domingo - Verão" -- OS de Verão
        WHEN data = "2023-12-16" THEN "Sabado - Verão" -- OS de Verão
        WHEN data = "2023-12-17" THEN "Domingo - Verão" -- OS de Verão
        WHEN data = "2024-01-06" THEN "Sabado - Verão" -- OS de Verão
        WHEN data = "2024-01-07" THEN "Domingo - Verão" -- OS de Verão
        WHEN data = "2024-02-09" THEN "Ponto Facultativo" -- Despacho MTR-DES-2024/07951
        WHEN data = "2024-02-12" THEN "Domingo" -- Despacho MTR-DES-2024/07951
        WHEN data = "2024-02-13" THEN "Domingo" -- Despacho MTR-DES-2024/07951
        WHEN data = "2024-02-14" THEN "Ponto Facultativo" -- Despacho MTR-DES-2024/07951
        WHEN data = "2023-12-31" THEN "Domingo - Réveillon"
        WHEN data = "2024-01-01" THEN "Domingo - Réveillon"
        WHEN data = "2024-02-24" THEN "Sabado - Verão" -- OS de Verão - Despacho MTR-DES-2024/10516
        WHEN data = "2024-02-25" THEN "Domingo - Verão" -- OS de Verão - Despacho MTR-DES-2024/10516
        WHEN data = "2024-03-16" THEN "Sabado - Verão" -- OS de Verão - Despacho MTR-DES-2024/15504
        WHEN data = "2024-03-17" THEN "Domingo - Verão" -- OS de Verão - Despacho MTR-DES-2024/15504
        WHEN data = "2024-03-22" THEN "Ponto Facultativo" -- Ponto Facultativo - DECRETO RIO Nº 54114/2024
        WHEN data = "2024-03-28" THEN "Ponto Facultativo" -- Ponto Facultativo - DECRETO RIO Nº 54081/2024
        WHEN data = "2024-03-29" THEN "Domingo" -- Feriado de Paixão de Cristo (Sexta-feira Santa)
        WHEN EXTRACT(DAY FROM data) = 20 AND EXTRACT(MONTH FROM data) = 1 THEN "Domingo" -- Dia de São Sebastião -- Art. 8°, I - Lei Municipal nº 5146/2010
        WHEN EXTRACT(DAY FROM data) = 23 AND EXTRACT(MONTH FROM data) = 4 THEN "Domingo" -- Dia de São Jorge -- Art. 8°, II - Lei Municipal nº 5146/2010 / Lei Estadual Nº 5198/2008 / Lei Estadual Nº 5645/2010
        WHEN EXTRACT(DAY FROM data) = 20 AND EXTRACT(MONTH FROM data) = 11 THEN "Domingo" -- Aniversário de morte de Zumbi dos Palmares / Dia da Consciência Negra -- Art. 8°, IV - Lei Municipal nº 5146/2010 / Lei Estadual nº 526/1982 / Lei Estadual nº 1929/1991 / Lei Estadual nº 4007/2002 / Lei Estadual Nº 5645/2010
        WHEN EXTRACT(DAY FROM data) = 21 AND EXTRACT(MONTH FROM data) = 4 THEN "Domingo" -- Tiradentes -- Art. 1º, VI - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAY FROM data) = 1 AND EXTRACT(MONTH FROM data) = 5 THEN "Domingo" -- Dia Mundial do Trabalho -- Art. 1º, VII - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAY FROM data) = 7 AND EXTRACT(MONTH FROM data) = 9 THEN "Domingo" -- Independência do Brasil -- Art. 1º, IX - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAY FROM data) = 12 AND EXTRACT(MONTH FROM data) = 10 THEN "Domingo" -- Nossa Senhora Aparecida -- Art. 1º, X - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAY FROM data) = 2 AND EXTRACT(MONTH FROM data) = 11 THEN "Domingo" -- Finados -- Art. 1º, XII - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAY FROM data) = 15 AND EXTRACT(MONTH FROM data) = 11 THEN "Domingo" -- Proclamação da República -- Art. 1º, XIII - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAY FROM data) = 25 AND EXTRACT(MONTH FROM data) = 12 THEN "Domingo" -- Natal -- Art. 1º, XIV - PORTARIA ME Nº 11.090/2022
        WHEN EXTRACT(DAYOFWEEK FROM data) = 1 THEN "Domingo"
        WHEN EXTRACT(DAYOFWEEK FROM data) = 7 THEN "Sabado"
        ELSE "Dia Útil"
    END AS tipo_dia,
    CASE
      -- Reveillon 2022:
      WHEN data = DATE(2022,12,31) THEN data
      WHEN data = DATE(2023,1,1) THEN data
      WHEN data BETWEEN DATE(2023,1,2) AND DATE(2023,1,15) THEN DATE(2023,1,2)
      -- Reprocessamento:
      WHEN data BETWEEN DATE(2023,1,15) AND DATE(2023,1,31) THEN DATE(2023,1,16)
      WHEN data BETWEEN DATE(2023,3,16) AND DATE(2023,3,31) THEN DATE(2023,3,16)
      -- Alteração de Planejamento
      WHEN data BETWEEN DATE(2023,6,16) AND DATE(2023,6,30) THEN DATE(2023,6,16)
      WHEN data BETWEEN DATE(2023,7,16) AND DATE(2023,7,31) THEN DATE(2023,7,16)
      WHEN data BETWEEN DATE(2023,8,16) AND DATE(2023,8,31) THEN DATE(2023,8,16)
      WHEN data BETWEEN DATE(2023,9,16) AND DATE(2023,9,30) THEN DATE(2023,9,16)
      WHEN data BETWEEN DATE(2023,10,16) AND DATE(2023,10,16) THEN DATE(2023,10,16)
      WHEN data BETWEEN DATE(2023,10,17) AND DATE(2023,10,23) THEN DATE(2023,10,17)
      WHEN data BETWEEN DATE(2023,10,24) AND DATE(2023,10,31) THEN DATE(2023,10,24)
      WHEN data = DATE(2023,12,01) THEN data -- Desvio do TIG
      WHEN data BETWEEN DATE(2023,12,02) AND DATE(2023,12,03) THEN DATE(2023,12,03) -- OS de Verão
      WHEN data BETWEEN DATE(2023,12,16) AND DATE(2023,12,17) THEN DATE(2023,12,03) -- OS de Verão
      WHEN data BETWEEN DATE(2023,12,04) AND DATE(2023,12,20) THEN DATE(2023,12,02) -- Fim do desvio do TIG
      WHEN data BETWEEN DATE(2023,12,21) AND DATE(2023,12,30) THEN DATE(2023,12,21)
      -- Reveillon 2023:
      WHEN data = DATE(2023,12,31) THEN data
      WHEN data = DATE(2024,01,01) THEN data
      -- 2024:
      WHEN data BETWEEN DATE(2024,01,06) AND DATE(2024,01,07) THEN DATE(2024,01,03) -- OS de Verão
      WHEN data BETWEEN DATE(2024,01,02) AND DATE(2024,01,14) THEN DATE(2024,01,02)
      WHEN data BETWEEN DATE(2024,01,15) AND DATE(2024,01,31) THEN DATE(2024,01,15)
      WHEN data BETWEEN DATE(2024,02,01) AND DATE(2024,02,18) THEN DATE(2024,02,01) -- OS fev/Q1
      WHEN data BETWEEN DATE(2024,02,19) AND DATE(2024,02,23) THEN DATE(2024,02,19) -- OS fev/Q2
      WHEN data BETWEEN DATE(2024,02,24) AND DATE(2024,02,25) THEN DATE(2024,02,25) -- OS fev/Q2 - TIG - OS Verão
      WHEN data BETWEEN DATE(2024,02,26) AND DATE(2024,03,01) THEN DATE(2024,02,24) -- OS fev/Q2 - TIG
      WHEN data BETWEEN DATE(2024,03,02) AND DATE(2024,03,10) THEN DATE(2024,03,02) -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,11) AND DATE(2024,03,15) THEN DATE(2024,03,11) -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,16) AND DATE(2024,03,17) THEN DATE(2024,03,12) -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,18) AND DATE(2024,03,29) THEN DATE(2024,03,18) -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,30) AND DATE(2024,04,30) THEN DATE(2024,03,30) -- OS abr/Q1
      -- 2022:
      WHEN data BETWEEN DATE(2022,10,1) AND DATE(2022,10,2) THEN DATE(2022,9,16)
      WHEN data BETWEEN DATE(2022,6,1) AND LAST_DAY(DATE(2022,6,30), MONTH) THEN DATE(2022,6,1)
      {% for i in range(7, 13) %}
        WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
        WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
      {% endfor %}
      -- 2023 a 2024:
      {% for j in range(2023, 2025) %}
        {% for i in range(1, 13) %}
          WHEN EXTRACT(MONTH FROM data) = {{ i }} AND EXTRACT(YEAR FROM data) = {{ j }} THEN DATE({{ j }},{{ i }},1)
        {% endfor %}
      {% endfor %}
    END AS data_versao_trips,
    CASE
      -- Reveillon 2022:
      WHEN data = DATE(2022,12,31) THEN data
      WHEN data = DATE(2023,1,1) THEN data
      WHEN data BETWEEN DATE(2023,1,2) AND DATE(2023,1,15) THEN DATE(2023,1,2)
      -- Reprocessamento:
      WHEN data BETWEEN DATE(2023,1,15) AND DATE(2023,1,31) THEN DATE(2023,1,16)
      WHEN data BETWEEN DATE(2023,3,16) AND DATE(2023,3,31) THEN DATE(2023,3,16)
      -- Alteração de Planejamento
      WHEN data BETWEEN DATE(2023,6,16) AND DATE(2023,6,30) THEN DATE(2023,6,16)
      WHEN data BETWEEN DATE(2023,7,16) AND DATE(2023,7,31) THEN DATE(2023,7,16)
      WHEN data BETWEEN DATE(2023,8,16) AND DATE(2023,8,31) THEN DATE(2023,8,16)
      WHEN data BETWEEN DATE(2023,9,16) AND DATE(2023,9,30) THEN DATE(2023,9,16)
      WHEN data BETWEEN DATE(2023,10,16) AND DATE(2023,10,16) THEN DATE(2023,10,16)
      WHEN data BETWEEN DATE(2023,10,17) AND DATE(2023,10,23) THEN DATE(2023,10,17)
      WHEN data BETWEEN DATE(2023,10,24) AND DATE(2023,10,31) THEN DATE(2023,10,24)
      WHEN data = DATE(2023,12,01) THEN data -- Desvio do TIG
      WHEN data BETWEEN DATE(2023,12,02) AND DATE(2023,12,03) THEN DATE(2023,12,03) -- OS de Verão
      WHEN data BETWEEN DATE(2023,12,16) AND DATE(2023,12,17) THEN DATE(2023,12,03) -- OS de Verão
      WHEN data BETWEEN DATE(2023,12,04) AND DATE(2023,12,20) THEN DATE(2023,12,02) -- Fim do desvio do TIG
      WHEN data BETWEEN DATE(2023,12,21) AND DATE(2023,12,30) THEN DATE(2023,12,21)
      -- Reveillon 2023:
      WHEN data = DATE(2023,12,31) THEN data
      WHEN data = DATE(2024,01,01) THEN data
      -- 2024:
      WHEN data BETWEEN DATE(2024,01,06) AND DATE(2024,01,07) THEN DATE(2024,01,03) -- OS de Verão
      WHEN data BETWEEN DATE(2024,01,02) AND DATE(2024,01,14) THEN DATE(2024,01,02)
      WHEN data BETWEEN DATE(2024,01,15) AND DATE(2024,01,31) THEN DATE(2024,01,15)
      WHEN data BETWEEN DATE(2024,02,01) AND DATE(2024,02,18) THEN DATE(2024,02,01) -- OS fev/Q1
      WHEN data BETWEEN DATE(2024,02,19) AND DATE(2024,02,23) THEN DATE(2024,02,19) -- OS fev/Q2
      WHEN data BETWEEN DATE(2024,02,24) AND DATE(2024,02,25) THEN DATE(2024,02,25) -- OS fev/Q2 - TIG - OS Verão
      WHEN data BETWEEN DATE(2024,02,26) AND DATE(2024,03,01) THEN DATE(2024,02,24) -- OS fev/Q2 - TIG
      WHEN data BETWEEN DATE(2024,03,02) AND DATE(2024,03,10) THEN DATE(2024,03,02) -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,11) AND DATE(2024,03,15) THEN DATE(2024,03,11) -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,16) AND DATE(2024,03,17) THEN DATE(2024,03,12) -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,18) AND DATE(2024,03,29) THEN DATE(2024,03,18) -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,30) AND DATE(2024,04,30) THEN DATE(2024,03,30) -- OS abr/Q1
      -- 2022:
      WHEN data BETWEEN DATE(2022,10,1) AND DATE(2022,10,2) THEN DATE(2022,9,16)
      WHEN data BETWEEN DATE(2022,6,1) AND LAST_DAY(DATE(2022,6,30), MONTH) THEN DATE(2022,6,1)
      {% for i in range(7, 13) %}
        WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
        WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
      {% endfor %}
      -- 2023 a 2024:
      {% for j in range(2023, 2025) %}
        {% for i in range(1, 13) %}
          WHEN EXTRACT(MONTH FROM data) = {{ i }} AND EXTRACT(YEAR FROM data) = {{ j }} THEN DATE({{ j }},{{ i }},1)
        {% endfor %}
      {% endfor %}
    END AS data_versao_shapes,
    CASE
      -- Reveillon 2022:
      WHEN data = DATE(2022,12,31) THEN data
      WHEN data = DATE(2023,1,1) THEN data
      WHEN data BETWEEN DATE(2023,1,2) AND DATE(2023,1,15) THEN DATE(2023,1,2)
      -- Reprocessamento:
      WHEN data BETWEEN DATE(2023,1,15) AND DATE(2023,1,31) THEN DATE(2023,1,16)
      WHEN data BETWEEN DATE(2023,3,16) AND DATE(2023,3,31) THEN DATE(2023,3,16)
      -- Alteração de Planejamento
      WHEN data BETWEEN DATE(2023,6,16) AND DATE(2023,6,30) THEN DATE(2023,6,16)
      WHEN data BETWEEN DATE(2023,7,16) AND DATE(2023,7,31) THEN DATE(2023,7,16)
      WHEN data BETWEEN DATE(2023,8,16) AND DATE(2023,8,31) THEN DATE(2023,8,16)
      WHEN data BETWEEN DATE(2023,9,16) AND DATE(2023,9,30) THEN DATE(2023,9,16)
      WHEN data BETWEEN DATE(2023,10,16) AND DATE(2023,10,16) THEN DATE(2023,10,16)
      WHEN data BETWEEN DATE(2023,10,17) AND DATE(2023,10,23) THEN DATE(2023,10,17)
      WHEN data BETWEEN DATE(2023,10,24) AND DATE(2023,10,31) THEN DATE(2023,10,24)
      WHEN data = DATE(2023,12,01) THEN data -- Desvio do TIG
      WHEN data BETWEEN DATE(2023,12,02) AND DATE(2023,12,03) THEN DATE(2023,12,03) -- OS de Verão
      WHEN data BETWEEN DATE(2023,12,16) AND DATE(2023,12,17) THEN DATE(2023,12,03) -- OS de Verão
      WHEN data BETWEEN DATE(2023,12,04) AND DATE(2023,12,20) THEN DATE(2023,12,02) -- Fim do desvio do TIG
      WHEN data BETWEEN DATE(2023,12,21) AND DATE(2023,12,30) THEN DATE(2023,12,21)
      -- Reveillon 2023:
      WHEN data = DATE(2023,12,31) THEN data
      WHEN data = DATE(2024,01,01) THEN data
      -- 2024:
      WHEN data BETWEEN DATE(2024,01,06) AND DATE(2024,01,07) THEN DATE(2024,01,03) -- OS de Verão
      WHEN data BETWEEN DATE(2024,01,02) AND DATE(2024,01,14) THEN DATE(2024,01,02)
      WHEN data BETWEEN DATE(2024,01,15) AND DATE(2024,01,31) THEN DATE(2024,01,15)
      WHEN data BETWEEN DATE(2024,02,01) AND DATE(2024,02,18) THEN DATE(2024,02,01) -- OS fev/Q1
      WHEN data BETWEEN DATE(2024,02,19) AND DATE(2024,02,23) THEN DATE(2024,02,19) -- OS fev/Q2
      WHEN data BETWEEN DATE(2024,02,24) AND DATE(2024,02,25) THEN DATE(2024,02,25) -- OS fev/Q2 - TIG - OS Verão
      WHEN data BETWEEN DATE(2024,02,26) AND DATE(2024,03,01) THEN DATE(2024,02,24) -- OS fev/Q2 - TIG
      WHEN data BETWEEN DATE(2024,03,02) AND DATE(2024,03,10) THEN DATE(2024,03,02) -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,11) AND DATE(2024,03,15) THEN DATE(2024,03,11) -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,16) AND DATE(2024,03,17) THEN DATE(2024,03,12) -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,18) AND DATE(2024,03,29) THEN DATE(2024,03,18) -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,30) AND DATE(2024,04,30) THEN DATE(2024,03,30) -- OS abr/Q1
      -- 2022:
      {% for i in range(6, 13) %}
        WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
        WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
      {% endfor %}
      -- 2023 a 2024:
      {% for j in range(2023, 2025) %}
        {% for i in range(1, 13) %}
          WHEN EXTRACT(MONTH FROM data) = {{ i }} AND EXTRACT(YEAR FROM data) = {{ j }} THEN DATE({{ j }},{{ i }},1)
        {% endfor %}
      {% endfor %}
    END AS data_versao_frequencies,
    CASE  
      WHEN EXTRACT(YEAR FROM data) = 2022 THEN (
        CASE
          WHEN EXTRACT(MONTH FROM data) = 6 THEN 2.13
          WHEN EXTRACT(MONTH FROM data) = 7 THEN 1.84
          WHEN EXTRACT(MONTH FROM data) = 8 THEN 1.80
          WHEN EXTRACT(MONTH FROM data) = 9 THEN 1.75
          WHEN EXTRACT(MONTH FROM data) = 10 THEN 1.62
          WHEN EXTRACT(MONTH FROM data) = 11 THEN 1.53
          WHEN EXTRACT(MONTH FROM data) = 12 THEN 1.78
        END
      )
      WHEN EXTRACT(YEAR FROM data) = 2023 THEN (
        CASE
          WHEN data <= DATE("2023-01-06") THEN 3.18
          ELSE 2.81
        END
      )
    END AS valor_subsidio_por_km
  FROM UNNEST(GENERATE_DATE_ARRAY("2022-06-01", DATE_SUB("{{var('DATA_SUBSIDIO_V6_INICIO')}}", INTERVAL 1 DAY))) AS data),
  trips AS (
  SELECT
    DISTINCT data_versao
  FROM
    {{ ref("subsidio_trips_desaninhada") }} 
  {% if is_incremental() %}
  WHERE
    data_versao >= DATE_TRUNC(DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 30 DAY), MONTH)
  {% endif %}
  ),
  shapes AS (
  SELECT
    DISTINCT data_versao
  FROM
    {{ var("subsidio_shapes") }} 
  {% if is_incremental() %}
  WHERE
    data_versao >= DATE_TRUNC(DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 30 DAY), MONTH)
  {% endif %}
  ),
  frequencies AS (
  SELECT
    DISTINCT data_versao
  FROM
    {{ ref("subsidio_quadro_horario") }} 
  {% if is_incremental() %}
  WHERE
    data_versao >= DATE_TRUNC(DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 30 DAY), MONTH)
  {% endif %}
  )
SELECT
  data,
  tipo_dia,
  SAFE_CAST(NULL AS STRING) AS subtipo_dia,
  COALESCE(t.data_versao, DATE("{{ trips_date }}")) AS data_versao_trips,
  COALESCE(s.data_versao, DATE("{{ shapes_date }}")) AS data_versao_shapes,
  COALESCE(f.data_versao, DATE("{{ frequencies_date }}")) AS data_versao_frequencies,
  valor_subsidio_por_km,
  SAFE_CAST(NULL AS STRING) AS feed_version,
  SAFE_CAST(NULL AS DATE)AS feed_start_date,
  SAFE_CAST(NULL AS STRING) AS tipo_os,
FROM
  dates AS d
LEFT JOIN
  trips AS t
ON
  t.data_versao = d.data_versao_trips
LEFT JOIN
  shapes AS s
ON
  s.data_versao = d.data_versao_shapes
LEFT JOIN
  frequencies AS f
ON
  f.data_versao = d.data_versao_frequencies
WHERE
{% if is_incremental() %}
  data BETWEEN DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 DAY) AND DATE("{{ var("run_date") }}")
{% else %}
  data <= DATE("{{ var("run_date") }}")
{% endif %}

{% else %}
{% if execute %}
  {% set max_feed_version = run_query("SELECT feed_version FROM " ~ ref('feed_info_gtfs2') ~ " WHERE feed_start_date = (SELECT MAX(feed_start_date) FROM " ~ ref('feed_info_gtfs2') ~ " WHERE feed_start_date >= DATE_TRUNC(DATE_SUB(DATE('" ~ var("run_date") ~ "'), INTERVAL 30 DAY), MONTH))").columns[0].values()[0] %}
{% endif %}

WITH
  dates AS (
  SELECT 
    data,
    CASE
      WHEN EXTRACT(DAY FROM data) = 20 AND EXTRACT(MONTH FROM data) = 1 THEN "Domingo" -- Dia de São Sebastião -- Art. 8°, I - Lei Municipal nº 5146/2010
      WHEN EXTRACT(DAY FROM data) = 23 AND EXTRACT(MONTH FROM data) = 4 THEN "Domingo" -- Dia de São Jorge -- Art. 8°, II - Lei Municipal nº 5146/2010 / Lei Estadual Nº 5198/2008 / Lei Estadual Nº 5645/2010
      WHEN EXTRACT(DAY FROM data) = 20 AND EXTRACT(MONTH FROM data) = 11 THEN "Domingo" -- Aniversário de morte de Zumbi dos Palmares / Dia da Consciência Negra -- Art. 8°, IV - Lei Municipal nº 5146/2010 / Lei Estadual nº 526/1982 / Lei Estadual nº 1929/1991 / Lei Estadual nº 4007/2002 / Lei Estadual Nº 5645/2010
      WHEN EXTRACT(DAY FROM data) = 21 AND EXTRACT(MONTH FROM data) = 4 THEN "Domingo" -- Tiradentes -- Art. 1º, VI - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAY FROM data) = 1 AND EXTRACT(MONTH FROM data) = 5 THEN "Domingo" -- Dia Mundial do Trabalho -- Art. 1º, VII - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAY FROM data) = 7 AND EXTRACT(MONTH FROM data) = 9 THEN "Domingo" -- Independência do Brasil -- Art. 1º, IX - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAY FROM data) = 12 AND EXTRACT(MONTH FROM data) = 10 THEN "Domingo" -- Nossa Senhora Aparecida -- Art. 1º, X - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAY FROM data) = 2 AND EXTRACT(MONTH FROM data) = 11 THEN "Domingo" -- Finados -- Art. 1º, XII - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAY FROM data) = 15 AND EXTRACT(MONTH FROM data) = 11 THEN "Domingo" -- Proclamação da República -- Art. 1º, XIII - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAY FROM data) = 25 AND EXTRACT(MONTH FROM data) = 12 THEN "Domingo" -- Natal -- Art. 1º, XIV - PORTARIA ME Nº 11.090/2022
      WHEN EXTRACT(DAYOFWEEK FROM data) = 1 THEN "Domingo"
      WHEN EXTRACT(DAYOFWEEK FROM data) = 7 THEN "Sabado"
      ELSE "Dia Útil"
    END AS tipo_dia,
    CASE
      WHEN data BETWEEN DATE(2024,03,11) AND DATE(2024,03,17) THEN "2024-03-11" -- OS mar/Q1
      WHEN data BETWEEN DATE(2024,03,18) AND DATE(2024,03,29) THEN "2024-03-18" -- OS mar/Q2
      WHEN data BETWEEN DATE(2024,03,30) AND DATE(2024,04,30) THEN "2024-03-30"  -- OS abr/Q1
      ELSE NULL
    END AS feed_version,
    CASE
      WHEN data BETWEEN DATE(2024,03,18) AND DATE(2024,03,31) THEN "Regular"
      ELSE "Regular"
    END AS tipo_os,
  FROM UNNEST(GENERATE_DATE_ARRAY("{{var('DATA_SUBSIDIO_V6_INICIO')}}", "2024-12-31")) AS data)
SELECT
  data,
  tipo_dia,
  CASE
    WHEN tipo_os = "Extraordinária - Verão" THEN "Verão"
    ELSE NULL
  END AS subtipo_dia,
  SAFE_CAST(NULL AS DATE) AS data_versao_trips,
  SAFE_CAST(NULL AS DATE) AS data_versao_shapes,
  SAFE_CAST(NULL AS DATE) AS data_versao_frequencies,
  SAFE_CAST(NULL AS FLOAT64) AS valor_subsidio_por_km,
  COALESCE(d.feed_version, "{{ max_feed_version }}") AS feed_version,
  feed_start_date,
  tipo_os,
FROM
  dates AS d
LEFT JOIN
  {{ ref('feed_info_gtfs2') }} AS i
ON
  CASE
    WHEN d.feed_version IS NULL AND "{{ max_feed_version }}" = i.feed_version THEN TRUE
    WHEN d.feed_version = i.feed_version THEN TRUE
  ELSE FALSE
  END
WHERE
{% if is_incremental() %}
  data = DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 DAY)
{% else %}
  data <= DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 DAY)
{% endif %}

{% endif %}