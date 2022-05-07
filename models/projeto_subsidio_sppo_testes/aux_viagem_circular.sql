-- 1. Seleciona servicos circulares registrados no quadro horário
with tipo_servico as (
    select 
        distinct servico, sentido
    from 
        {{ var("sppo_viagem_planejada") }}
),
-- 2. Seleciona viagens circulares (separadas em shape ida e volta)
viagem as (
    select 
        v.*
    from 
        {{ ref("aux_viagem_inicio_fim") }} v
    inner join 
        (select * from tipo_servico where sentido = "C") c
    on 
        v.servico_realizado = c.servico
    where 
        v.sentido = "I" or v.sentido = "V"
    order by 
        id_veiculo, servico_realizado, datetime_partida
),
-- 3. Identifica e junta viagens de ida seguidas de volta, recalcula tempo total da viagem
viagem_circular as (
    select 
        data,
        tipo_dia,
        id_veiculo,
        id_empresa,
        servico_informado,
        servico_realizado,
        CONCAT(SUBSTR(shape_id, 1, 10), "C", SUBSTR(shape_id, 12)) as shape_id,
        "C" as sentido,
        id_viagem,
        datetime_partida,
        datetime_chegada_volta as datetime_chegada,
        datetime_diff(datetime_chegada, datetime_partida, minute) + 1 as tempo_viagem
    from (
        select 
            *,
            lead(datetime_partida) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as datetime_partida_volta,
            lead(datetime_chegada) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as datetime_chegada_volta,
            lead(sentido) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) = "V" as flag_proximo_volta -- possui volta
        from 
            viagem
    ) t
    where
        flag_proximo_volta = TRUE
        and sentido = "I"
),
-- 4. Junta viagens circulares às viagens não circulares
viagem_nao_circular as (
    select 
        v.*
    from 
        {{ ref("aux_viagem_inicio_fim") }} v
    inner join 
        (select * from tipo_servico where sentido != "C") c
    on 
        v.servico_realizado = c.servico
)
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem_circular
union all (
    select 
        *
    from 
        viagem_nao_circular
)