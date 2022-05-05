-- 1. Calcula a distância total percorrida por viagem
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
                ordem_viagem
        end ordem_viagem
    from 
        {{ ref("aux_registros_status_viagem") }} s
    left join 
        {{ ref("aux_viagem_inicio_fim") }} v
    on 
        s.data = v.data
        and s.id_veiculo = v.id_veiculo
        and (s.timestamp_gps = v.datetime_partida or s.timestamp_gps = v.datetime_chegada)
        and s.shape_id = v.shape_id
),
registros as (
    select 
        * except(ordem_viagem),
        case
            when
                ordem_viagem is not null
            then 
                ordem_viagem
            when
                (timestamp_gps >= LAST_VALUE(datetime_partida IGNORE NULLS) over (
                        partition by id_veiculo, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
                and
                timestamp_gps <= LAST_VALUE(datetime_chegada IGNORE NULLS) over (
                        partition by id_veiculo, shape_id
                        order by timestamp_gps
                        rows between  unbounded preceding and current row
                    )
                )
            then 
                LAST_VALUE(ordem_viagem IGNORE NULLS) over (
                        partition by id_veiculo, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
        end ordem_viagem
    from aux_registros mt
),
distancia as (
    select 
        data,
        id_veiculo,
        id_empresa,
        servico_informado,
        servico_realizado,
        shape_id,
        ordem_viagem,
        distancia_teorica,
        round(sum(distancia)/1000, 2) distancia_aferida,
        count(case when flag_trajeto_correto is true then 1 end) n_registros_shape,
        count(timestamp_gps) n_registros, 
    from 
        registros 
    where 
        ordem_viagem is not null
    group by 
        1,2,3,4,5,6,7,8
    order by 
        data, id_veiculo, shape_id, ordem_viagem
),
-- 2. Calcula os percentuais de conformidade da viagem
conformidade as (
    select 
        v.* except(ordem_viagem, versao_modelo),
        distancia_teorica,
        distancia_aferida,
        n_registros_shape,
        n_registros,
        round(n_registros_shape/n_registros*100,2) as perc_conformidade_shape,
        round(100 * (d.distancia_aferida/d.distancia_teorica), 2) as perc_conformidade_distancia,
        round(n_registros/tempo_viagem*100, 2) as perc_conformidade_registros
    from 
        {{ ref("aux_viagem_inicio_fim") }} v
    inner join 
        distancia d
    on
        v.data = d.data
        and v.id_veiculo = d.id_veiculo
        and v.shape_id = d.shape_id
        and v.ordem_viagem = d.ordem_viagem
    order by
        data, id_veiculo, shape_id, datetime_partida
)
select 
    c.*,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    conformidade c