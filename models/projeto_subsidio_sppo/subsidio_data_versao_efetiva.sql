-- TODO: parametrizar
select 
  data,
  case
      when extract(dayofweek from data) = 1 then 'Domingo'
      when extract(dayofweek from data) = 7 then 'Sabado'
      else 'Dia Útil'
  end as tipo_dia,
  case  
    when data between "2022-06-01" and "2022-06-15" then date("2022-06-16")
  end as data_versao_sigmob
from unnest(GENERATE_DATE_ARRAY("2022-06-01", "2022-12-31")) data