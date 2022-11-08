
{{ config(
       materialized='incremental',
       partition_by={
            "field":"data_viagem",
            "data_type": "date",
            "granularity":"day"
       },
       unique_key="data_viagem",
       incremental_strategy = 'insert_overwrite'
)
}}

with recursos as (
  select *
  FROM {{ ref("aux_recurso_duplicado") }}
  where id_julgamento is null
),
-- 1. Avalia recursos cuja viagem ja foi paga
viagens as (
    select * 
    from `rj-smtr-dev.projeto_subsidio_sppo.viagem_completa`
    {# {% if is_incremental() -%} #}
      where data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
    {# {% endif -%} #}
),
recursos_pagos as (
  select
    r.protocolo,
    4 as id_julgamento,
    concat("Viagem(s) paga(s): ", string_agg(v.id_viagem, ", ")) as observacao
  FROM recursos r
  inner join viagens v
  on r.id_veiculo = substr(v.id_veiculo,2,6)
  and r.datetime_partida <= v.datetime_partida and r.datetime_chegada >= v.datetime_chegada
  group by 1,2
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from {{ ref("aux_recurso_duplicado") }} r
left join recursos_pagos rp
on r.protocolo = rp.protocolo