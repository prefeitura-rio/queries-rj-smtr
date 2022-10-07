with recursos as (
  select *
  FROM {{ ref("aux_recurso_indeferido_incorreto") }} -- `rj-smtr-dev.projeto_subsidio_sppo.aux_recurso_indeferido_incorreto`
  where julgamento is null
),
viagens as (
    select * 
    from `rj-smtr-dev.projeto_subsidio_sppo.viagem_completa` -- TODO: ref in prod
    -- TODO: incremental
    where data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
),
-- Avalia recursos cuja viagem ja foi paga
recursos_pagos as (
  select
    r.protocolo,
    concat("Existe viagem paga no horário: ", string_agg(v.id_viagem, ", ")) as observacao,
    "Indeferido" as julgamento,
    "Viagem já paga." as motivo
  FROM recursos r
  inner join viagens v
  on r.id_veiculo = substr(v.id_veiculo,2,6)
  and r.datetime_partida <= v.datetime_partida and r.datetime_chegada >= v.datetime_chegada
  group by 1
),
-- Avalia recursos com sobreposição
recursos_sobrepostos as (
    SELECT
      s1.protocolo,
      concat("AÇÃO NECESSÁRIA: Existe sopreposição da viagem com o(s) seguinte(s) recurso(s) - ", string_agg(s2.protocolo, ", "), ". Avalie qual o correto e cancele os demais.") as observacao,
      "" as julgamento,
      s1.motivo
    FROM recursos s1
    inner join recursos s2
    on 
      s1.id_veiculo = s2.id_veiculo
      and s1.datetime_partida <= s2.datetime_partida
      and s2.datetime_partida < s1.datetime_chegada
      and s1.id_recurso != s2.id_recurso
    where s1.protocolo not in (select protocolo from recursos_pagos)
    group by 1,3,4
)
select
  r.* except(observacao, julgamento, motivo, distancia_paga),
  coalesce(r.observacao, s.observacao, p.observacao) as observacao,
  coalesce(r.julgamento, s.julgamento, p.julgamento) as julgamento,
  coalesce(r.motivo, s.motivo, p.motivo) as motivo,
  distancia_paga
from {{ ref("aux_recurso_indeferido_incorreto") }} r -- `rj-smtr-dev.projeto_subsidio_sppo.aux_recurso_indeferido_incorreto` r
left join recursos_pagos p
on r.protocolo = p.protocolo
left join recursos_sobrepostos s
on r.protocolo = s.protocolo