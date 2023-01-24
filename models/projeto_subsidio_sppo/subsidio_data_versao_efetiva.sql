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
      WHEN EXTRACT(DAYOFWEEK FROM data) = 1 THEN "Domingo"
      WHEN EXTRACT(DAYOFWEEK FROM data) = 7 THEN "Sabado"
      ELSE 'Dia Útil'
  END AS tipo_dia,
  CASE
    WHEN data BETWEEN DATE(2022,10,1) AND DATE(2022,10,2) THEN DATE(2022,9,16)
    WHEN data BETWEEN DATE(2022,6,1) AND LAST_DAY(DATE(2022,6,30), MONTH) THEN DATE(2022,6,1)
    {% for i in range(7, 12) %}
      WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
      WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
    {% endfor %}
    WHEN data BETWEEN DATE(2022,12,1) AND DATE(2022,12,15) THEN DATE(2022,12,1)
    WHEN data BETWEEN DATE(2022,12,16) AND DATE(2022,12,30) THEN DATE(2022,12,16)
    WHEN data BETWEEN DATE(2022,12,31) AND DATE(2023,1,1) THEN DATE(2022,12,31)
    {% for j in range(2023, 2024) %}
      WHEN data BETWEEN DATE({{ j }},1,2) AND DATE({{ j }},1,15) THEN DATE({{ j }},1,1)
      WHEN data BETWEEN DATE({{ j }},1,16) AND LAST_DAY(DATE({{ j }},1,1), MONTH) THEN DATE({{ j }},1,16)
      {% for i in range(2, 13) %}
        WHEN data BETWEEN DATE({{ j }},{{ i }},1) AND DATE({{ j }},{{ i }},15) THEN DATE({{ j }},{{ i }},1)
        WHEN data BETWEEN DATE({{ j }},{{ i }},16) AND LAST_DAY(DATE({{ j }},{{ i }},1), MONTH) THEN DATE({{ j }},{{ i }},16)
      {% endfor %}
    {% endfor %}
  END AS data_versao_trips,
  CASE
    WHEN data BETWEEN DATE(2022,10,1) AND DATE(2022,10,2) THEN DATE(2022,9,16)
    WHEN data BETWEEN DATE(2022,6,1) AND LAST_DAY(DATE(2022,6,30), MONTH) THEN DATE(2022,6,1)
    {% for i in range(7, 12) %}
      WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
      WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
    {% endfor %}
    WHEN data BETWEEN DATE(2022,12,1) AND DATE(2022,12,15) THEN DATE(2022,12,1)
    WHEN data BETWEEN DATE(2022,12,16) AND DATE(2022,12,30) THEN DATE(2022,12,16)
    WHEN data BETWEEN DATE(2022,12,31) AND DATE(2023,1,1) THEN DATE(2022,12,31)
    {% for j in range(2023, 2024) %}
      WHEN data BETWEEN DATE({{ j }},1,2) AND DATE({{ j }},1,15) THEN DATE({{ j }},1,1)
      WHEN data BETWEEN DATE({{ j }},1,16) AND LAST_DAY(DATE({{ j }},1,1), MONTH) THEN DATE({{ j }},1,16)
      {% for i in range(2, 13) %}
        WHEN data BETWEEN DATE({{ j }},{{ i }},1) AND DATE({{ j }},{{ i }},15) THEN DATE({{ j }},{{ i }},1)
        WHEN data BETWEEN DATE({{ j }},{{ i }},16) AND LAST_DAY(DATE({{ j }},{{ i }},1), MONTH) THEN DATE({{ j }},{{ i }},16)
      {% endfor %}
    {% endfor %}
  END AS data_versao_shapes,
  CASE
    {% for i in range(6, 12) %}
      WHEN data BETWEEN DATE(2022,{{ i }},1) AND DATE(2022,{{ i }},15) THEN DATE(2022,{{ i }},1)
      WHEN data BETWEEN DATE(2022,{{ i }},16) AND LAST_DAY(DATE(2022,{{ i }},30), MONTH) THEN DATE(2022,{{ i }},16)
    {% endfor %}
    WHEN data BETWEEN DATE(2022,12,1) AND DATE(2022,12,15) THEN DATE(2022,12,1)
    WHEN data BETWEEN DATE(2022,12,16) AND DATE(2022,12,30) THEN DATE(2022,12,16)
    WHEN data BETWEEN DATE(2022,12,31) AND DATE(2023,1,1) THEN DATE(2022,12,31)
    {% for j in range(2023, 2024) %}
      WHEN data BETWEEN DATE({{ j }},1,2) AND DATE({{ j }},1,15) THEN DATE({{ j }},1,1)
      WHEN data BETWEEN DATE({{ j }},1,16) AND LAST_DAY(DATE({{ j }},1,1), MONTH) THEN DATE({{ j }},1,16)
      {% for i in range(2, 13) %}
        WHEN data BETWEEN DATE({{ j }},{{ i }},1) AND DATE({{ j }},{{ i }},15) THEN DATE({{ j }},{{ i }},1)
        WHEN data BETWEEN DATE({{ j }},{{ i }},16) AND LAST_DAY(DATE({{ j }},{{ i }},1), MONTH) THEN DATE({{ j }},{{ i }},16)
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
