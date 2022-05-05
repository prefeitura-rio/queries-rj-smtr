{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       }
)
}}

-- 1. Seleciona servicos nao circulares registrados no quadro horário
with servico_nao_circular as (
    select 
        distinct servico
    from 
        {{ var("sppo_quadro_horario") }}
    where sentido != "C"
),
-- 2. Seleciona viagens nao circulares
viagem as (
    select 
        v.* except(versao_modelo)
    from 
        {{ ref("aux_viagem_conformidade") }} v
    inner join 
        servico_nao_circular c
    on 
        v.servico_realizado = c.servico
)
-- 2. Filtra viagens não circulares completas i.e. com percentual de conformidade válido
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem
where (
    perc_conformidade_distancia >= {{ var("perc_conformidade_distancia_min") }}
    and perc_conformidade_distancia <= {{ var("perc_conformidade_distancia_max") }}
)
and (
    perc_conformidade_shape >= {{ var("perc_conformidade_shape_min") }}
    and perc_conformidade_shape <= {{ var("perc_conformidade_shape_max") }}
)
and (
    perc_conformidade_registros >= {{ var("perc_conformidade_registros_min") }}
    and perc_conformidade_registros <= {{ var("perc_conformidade_registros_max") }}
)