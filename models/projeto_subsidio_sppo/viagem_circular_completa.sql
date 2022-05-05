{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       }
)
}}

-- 1. Seleciona servicos circulares registrados no quadro horário
with servico_circular as (
    select 
        distinct servico
    from 
        {{ var("sppo_quadro_horario") }}
    where 
        sentido = "C"
),
-- 2. Seleciona viagens circulares (separadas em shape ida e volta)
viagem as (
    select 
        v.*
    from 
        {{ ref("aux_viagem_conformidade") }} v
    inner join 
        servico_circular c
    on 
        v.servico_realizado = c.servico
    where 
        v.sentido = "I" or v.sentido = "V"
    order by 
        id_veiculo, servico_realizado, datetime_partida
),
-- 3. Identifica ida/volta consecutiva e agrega como viagem circular
aux_volta as (
    select 
        v.*,
        lead(datetime_partida) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as datetime_partida_volta,
        lead(datetime_chegada) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as datetime_chegada_volta,
        lead(tempo_viagem) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as tempo_viagem_volta,
        lead(distancia_teorica) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as distancia_teorica_volta,
        lead(distancia_aferida) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as distancia_aferida_volta,
        lead(n_registros_shape) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as n_registros_shape_volta,
        lead(n_registros) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as n_registros_volta,
        lead(sentido) over (
            partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) = "V" as flag_proximo_volta -- possui volta
    from 
        viagem v
),
viagem_circular as (
    select 
        v.data,
        v.tipo_dia,
        v.id_veiculo,
        v.id_empresa,
        v.servico_informado,
        v.servico_realizado,
        CONCAT(SUBSTR(v.shape_id, 1, 10), "C", SUBSTR(v.shape_id, 12)) as shape_id,
        "C" as sentido,
        v.datetime_partida,
        a.datetime_chegada_volta as datetime_chegada,
        (a.tempo_viagem + a.tempo_viagem_volta) as tempo_viagem,
        (a.distancia_teorica + a.distancia_teorica_volta) as distancia_teorica,
        (a.distancia_aferida + a.distancia_aferida_volta) as distancia_aferida,
        (a.n_registros_shape + a.n_registros_shape_volta) as n_registros_shape,
        (a.n_registros + a.n_registros_volta) as n_registros,
    from 
        viagem v
    inner join (
        select * 
        from aux_volta
        where
            flag_proximo_volta = TRUE
            and sentido = "I"
    ) a
    on (
        v.id_veiculo = a.id_veiculo
        and v.servico_realizado = a.servico_realizado
        and v.datetime_partida = a.datetime_partida
    )
),
-- 4. Calcula os percentuais de conformidade da viagem
conformidade as (
    select 
        v.*,
        round(n_registros_shape/n_registros*100,2) perc_conformidade_shape,
        round(100 * (distancia_aferida/distancia_teorica), 2) as perc_conformidade_distancia,
        round(n_registros/tempo_viagem*100, 2) as perc_conformidade_registros
    from 
        viagem_circular v
)
-- 5. Filtra viagens circulares completas i.e. com percentual de conformidade válido
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from conformidade
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