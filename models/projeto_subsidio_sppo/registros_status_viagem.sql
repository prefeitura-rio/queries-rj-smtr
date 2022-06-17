select 
    *
from 
    {{ ref("aux_registros_status_viagem_circular") }}
union all (
    select 
        *
    from 
        {{ ref("aux_registros_status_viagem_ida") }}
)
union all (
    select 
        *
    from 
        {{ ref("aux_registros_status_viagem_volta") }}
)