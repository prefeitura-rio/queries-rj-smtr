{{ 
config(
    materialized='ephemeral'
)
}}

with recursos as (
  select *
  FROM {{ ref("aux_recurso_viagem_nao_planejada") }}
  where id_julgamento is null
),
recurso_prazo as (
  select 
    r.protocolo,
    2 as id_julgamento,
    concat("Prazo (30 dias após apuração):", data_fim_recurso) as observacao
  from 
    recursos r
  inner join 
    {{ var("recurso_prazo") }} p
  on 
    date(r.datetime_partida) between p.data_inicio_viagem and p.data_fim_viagem
  where extract(date from data_recurso) > date(data_fim_recurso)
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from {{ ref("aux_recurso_viagem_nao_planejada") }} r 
left join recurso_prazo rp
on r.protocolo = rp.protocolo
