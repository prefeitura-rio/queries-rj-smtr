with sumario as (
  SELECT
    r.consorcio,
    r.data,
    r.servico,
    s.viagens_subsidio as viagens_subsidio_pre_recurso,
    s.perc_distancia_total_subsidio as perc_distancia_total_subsidio_pre_recurso,
    s.valor_total_subsidio as valor_total_subsidio_pre_recurso,
    r.viagens_subsidio as viagens_subsidio_pos_recurso,
    r.perc_distancia_total_subsidio as perc_distancia_total_subsidio_pos_recurso,
    r.valor_total_subsidio as valor_total_subsidio_pos_recurso
  FROM {{ ref("sumario_subsidio_dia_recurso") }} r
  join `rj-smtr.projeto_subsidio_sppo.sumario_subsidio_dia` s -- todo: ref to prod
  on r.data = s.data
  and r.servico = s.servico
)
select 
  *,
  round(valor_total_subsidio_pos_recurso - valor_total_subsidio_pre_recurso, 2) as valor_total_subsidio_recurso
from sumario