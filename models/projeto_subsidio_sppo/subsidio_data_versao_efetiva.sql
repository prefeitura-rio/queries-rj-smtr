select 
  data,
  case
      when data = "2022-06-16" then "Domingo"
      when data = "2022-06-17" then "Sabado"
      when extract(dayofweek from data) = 1 then 'Domingo'
      when extract(dayofweek from data) = 7 then 'Sabado'
      else 'Dia Útil'
  end as tipo_dia,
  case
    {% for i in range(6, 13) %}
      when data between date(2022,{{ i }},1) and last_day(date(2022,{{ i }},30), month) then date(2022,{{ i }},01)
      -- when data between date(2022,{{ i }},16) and date(2022,{{ i }},30) then date(2022,{{ i }},16)
    {% endfor %}
  end as data_versao_trips,
  case
    {% for i in range(6, 13) %}
      when data between date(2022,{{ i }},1) and last_day(date(2022,{{ i }},30), month) then date(2022,{{ i }},01)
      -- when data between date(2022,{{ i }},16) and date(2022,{{ i }},30) then date(2022,{{ i }},16)
    {% endfor %}
  end as data_versao_shapes,
  case
    {% for i in range(6, 13) %}
      when data between date(2022,{{ i }},1) and date(2022,{{ i }},15) then date(2022,{{ i }},01)
      when data between date(2022,{{ i }},16) and last_day(date(2022,{{ i }},30), month) then date(2022,{{ i }},16)
    {% endfor %}
  end as data_versao_frequencies,
  case  
    when extract(month from data) = 6 then 2.13
    when extract(month from data) = 7 then 1.84
    when extract(month from data) = 8 then 1.80
    when extract(month from data) = 9 then 1.75
    when extract(month from data) = 10 then 1.62
    when extract(month from data) = 11 then 1.53
    when extract(month from data) = 12 then 1.78
  end as valor_subsidio_por_km
from unnest(GENERATE_DATE_ARRAY("2022-06-01", "2022-12-31")) data