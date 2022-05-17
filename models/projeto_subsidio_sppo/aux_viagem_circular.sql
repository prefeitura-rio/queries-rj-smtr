-- 1. Identifica viagens de ida seguidas de volta circulares
with ida_volta_circular as (
    select 
        t.*
    from (
        select 
            *,
            lead(datetime_partida) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido_shape) as datetime_partida_volta,
            lead(datetime_chegada) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido_shape) as datetime_chegada_volta,
            lead(shape_id) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido_shape) as shape_id_volta,
            lead(sentido_shape) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido_shape) = "V" as flag_proximo_volta -- possui volta
        from 
            {{ ref("aux_viagem_inicio_fim") }} v
        where
            sentido = "C"
    ) t
    where
        flag_proximo_volta = TRUE
        and sentido_shape = "I"
        and datetime_chegada <= datetime_partida_volta
)
-- 2. Ajusta id_viagem de ida e volta para serem o mesmopaa viagens
--    circulares (por padrão, id_viagem com shape de ida)
circular as (
    select distinct
        * 
    from (
        select
            case
                when (
                    v.sentido_shape = "I"
                    and v.datetime_partida = c.datetime_partida
                ) then c.id_viagem
                when (
                    v.sentido_shape = "V"
                    and v.datetime_chegada = c.datetime_chegada_volta
                ) then c.id_viagem
            end as id_viagem,
            v.* except(id_viagem)
        from 
            {{ ref("aux_viagem_inicio_fim") }} v
        inner join 
            viagem_circular c
        on
            c.id_veiculo = v.id_veiculo
            and c.servico_realizado = v.servico_realizado
            and c.sentido = v.sentido
    ) v
    where
        id_viagem is not null
)
-- 3. Junta viagens circulares tratadas às não circulares já identificadas
select
    *
from 
    circular v
union all (
    select
        *
    from
        {{ ref("aux_viagem_inicio_fim") }} v
    where 
        sentido = "I" or sentido = "V"
)