{{ 
config(
    materialized='ephemeral'
)
}}

with recursos as (
  select *
  FROM {{ ref("aux_recurso_incorreto") }}
  where id_julgamento is null
  
),
servico_planejado as (
    select data, servico, sentido
    FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
    {% if is_incremental() -%}
      where data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
    {% endif -%}
),
recursos_nao_planejados as (
    select
        r.protocolo,
        1 as id_julgamento,
        "Serviço e sentido não planejados no subsídio na respectiva data da viagem." as observacao
    FROM recursos r
    left join servico_planejado s
    on date(r.datetime_partida) = s.data
    and r.servico = s.servico
    and r.sentido = s.sentido
    where s.servico is null
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from {{ ref("aux_recurso_incorreto") }} r 
left join recursos_nao_planejados rp
on r.protocolo = rp.protocolo