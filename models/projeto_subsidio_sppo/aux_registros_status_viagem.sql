-- 1. Classifica todos os registos pertencentes a cada viagem
with aux_registros as (
    select 
        s.*,
        datetime_partida,
        datetime_chegada,
        case 
            when 
                timestamp_gps = datetime_partida
                or
                timestamp_gps = datetime_chegada
            then
                id_viagem
        end id_viagem
    from 
        {{ ref("aux_registros_status_trajeto") }} s
    left join 
        {{ ref("aux_viagem_circular") }} v
    on 
        s.id_veiculo = v.id_veiculo
        and (s.timestamp_gps = v.datetime_partida or s.timestamp_gps = v.datetime_chegada)
        and s.servico_realizado = v.servico_realizado
        and s.sentido = v.sentido
),
registros_viagem as (
    select 
        * except(id_viagem, versao_modelo),
        case
            when
                id_viagem is not null
            then 
                id_viagem
            when
                (timestamp_gps >= LAST_VALUE(datetime_partida IGNORE NULLS) over (
                        partition by id_veiculo, servico_realizado, sentido
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
                and
                timestamp_gps <= LAST_VALUE(datetime_chegada IGNORE NULLS) over (
                        partition by id_veiculo, servico_realizado, sentido
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
                )
            then 
                LAST_VALUE(id_viagem IGNORE NULLS) over (
                        partition by id_veiculo, servico_realizado, sentido
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
        end id_viagem
    from aux_registros
)
-- 2. Filtra apenas registros de viagens identificadas
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    registros_viagem
where 
    id_viagem is not null
