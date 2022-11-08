{{ 
config(
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

-- Adiciona julgamento aos recursos restantes
with recursos as (
  select
    r.* except(id_julgamento),
    ifnull(id_julgamento, 9) as id_julgamento
  from {{ ref("aux_recurso_viagem_recalculada") }} r
  {# {% if is_incremental() -%} #}
  where data_viagem between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
  {# {% endif -%} #}
)
-- Preenche campos de julgamento e motivo com base na id_julgamento
select
  r.* except(observacao),
  d.julgamento,
  d.motivo,
  r.observacao
from recursos r
left join (
  select * from {{ var("recurso_julgamento") }}
) d
on r.id_julgamento = d.id_julgamento