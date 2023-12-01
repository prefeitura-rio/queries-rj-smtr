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
    -- Reveillon:
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
    -- 2022:
    WHEN data BETWEEN DATE(2022,10,1) AND DATE(2022,10,2) THEN DATE(2022,9,16)
    WHEN data BETWEEN DATE(2022,6,1) AND LAST_DAY(DATE(2022,6,30), MONTH) THEN DATE(2022,6,1)
    {% for i in range(7, 13) %}
      WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
      WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
    {% endfor %}
    -- 2023:
    {% for j in range(2023, 2024) %}
      {% for i in range(1, 13) %}
        WHEN EXTRACT(MONTH FROM data) = {{ i }} THEN DATE({{ j }},{{ i }},1)
      {% endfor %}
    {% endfor %}
  END AS data_versao_trips,
  CASE
    -- Reveillon:
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
    -- 2022:
    WHEN data BETWEEN DATE(2022,10,1) AND DATE(2022,10,2) THEN DATE(2022,9,16)
    WHEN data BETWEEN DATE(2022,6,1) AND LAST_DAY(DATE(2022,6,30), MONTH) THEN DATE(2022,6,1)
    {% for i in range(7, 13) %}
      WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
      WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
    {% endfor %}
    -- 2023:
    {% for j in range(2023, 2024) %}
      {% for i in range(1, 13) %}
        WHEN EXTRACT(MONTH FROM data) = {{ i }} THEN DATE({{ j }},{{ i }},1)
      {% endfor %}
    {% endfor %}
  END AS data_versao_shapes,
  CASE
    -- Reveillon:
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
    -- 2022:
    {% for i in range(6, 13) %}
      WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
      WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
    {% endfor %}
    -- 2023:
    {% for j in range(2023, 2024) %}
      {% for i in range(1, 13) %}
        WHEN EXTRACT(MONTH FROM data) = {{ i }} THEN DATE({{ j }},{{ i }},1)
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
FROM UNNEST(GENERATE_DATE_ARRAY("2022-06-01", "2023-12-31")) AS data
