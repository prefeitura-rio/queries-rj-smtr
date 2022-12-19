with sumario as (
  SELECT 
    consorcio,
    data,
    tipo_dia,
    servico,
    round(sum(viagens_planejadas), 3) as viagens_planejadas,
    round(sum(viagens_subsidio), 3) as viagens_subsidio,
    max(distancia_total_planejada) as distancia_total_planejada, -- distancia total do dia (junta ida+volta)
    round(sum(distancia_total_subsidio), 3) as distancia_total_subsidio,
  FROM {{ ref("sumario_subsidio_dia_periodo_recurso")}}
  group by 1,2,3,4
),
valor as (
  select
    s.*,
    v.valor_subsidio_por_km,
    round(distancia_total_subsidio * v.valor_subsidio_por_km, 2) as valor_total_aferido,
    round(100*distancia_total_subsidio/distancia_total_planejada, 2) as perc_distancia_total_subsidio
  from
    sumario s
  left join
    `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva` v -- {{ ref("subsidio_data_versao_efetiva")}} v
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