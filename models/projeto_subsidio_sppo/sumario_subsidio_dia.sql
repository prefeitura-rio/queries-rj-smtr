with sumario as (
  SELECT 
    consorcio,
    data,
    tipo_dia,
    servico,
    round(sum(viagens_planejadas), 3) as viagens_planejadas,
    round(sum(viagens_subsidio), 3) as viagens_subsidio,
    round(sum(distancia_total_planejada), 3) as distancia_total_planejada,
    round(sum(distancia_total_subsidio), 3) as distancia_total_subsidio,
  FROM {{ ref("sumario_subsidio_dia_periodo")}}
  group by 1,2,3,4
),
valor as (
  select
    s.*,
    v.valor_subsidio_por_km,
    round(distancia_total_subsidio * valor_subsidio_por_km, 2) as valor_total_aferido,
    round(100*distancia_total_subsidio/distancia_total_planejada, 2) as perc_distancia_total_subsidio
  from
    sumario s
  left join
    {{ var("valor_subsidio")}} v
  on v.data = s.data
)
select 
  *,
  case
    when perc_distancia_total_subsidio < {{ var("perc_distancia_total_subsidio") }}
    then 0
    else valor_total_aferido
  end as valor_total_subsidio
from valor