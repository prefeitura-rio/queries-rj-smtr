with viagens as (
    select 
        *,
        row_number() over (
            partition by id_veiculo, servico, shape_id
            order by datetime_partida
        ) trip_number
    from `rj-smtr-dev.projeto_subsidio_sppo_v2.viagens_realizadas`
    order by datetime_partida
),
merge_trip as (
    select 
        s.*,
        datetime_partida,
        datetime_chegada,
        -- trip_number
        CASE 
            WHEN 
                timestamp_gps = datetime_partida
                or
                timestamp_gps = datetime_chegada
            THEN
                trip_number
        END trip_number
    from rj-smtr-dev.projeto_subsidio_sppo_v2.aux_registros_status_viagem s
    left join viagens v
    on s.data = v.data
    and s.id_veiculo = v.id_veiculo
    and (s.timestamp_gps = v.datetime_partida or s.timestamp_gps = v.datetime_chegada)
    and s.shape_id = v.shape_id
    -- order by shape_id, timestamp_gps
),
classificacao as (
    select 
        * except(trip_number),
        CASE
            WHEN
                trip_number is not null
            THEN 
                trip_number
            WHEN
                (timestamp_gps >= LAST_VALUE(datetime_partida IGNORE NULLS) over (
                        partition by id_veiculo, servico, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
                and
                timestamp_gps <= LAST_VALUE(datetime_chegada IGNORE NULLS) over (
                        partition by id_veiculo, servico, shape_id
                        order by timestamp_gps
                        rows between  unbounded preceding and current row
                    )
                )
            THEN 
                LAST_VALUE(trip_number IGNORE NULLS) over (
                        partition by id_veiculo, servico, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
        END trip_number
    FROM merge_trip mt
),
distancia_estimada as (
    select 
        data,
        id_veiculo,
        servico,
        shape_id,
        trip_number,
        round(shape_distance/1000, 2) distancia_teorica,
        round(sum(distancia)/1000, 2) distancia_km,
        COUNT(CASE WHEN flag_trajeto_correto_hist is true then 1 end) flag_agg,
        COUNT(timestamp_gps) n_registros, 
    from classificacao 
    where trip_number is not null
    group by 1,2,3,4,5,6
order by data, id_veiculo, servico, shape_id, trip_number
),
conformidade as (
    select 
        v.*,
        distancia_teorica,
        distancia_km,
        round(flag_agg/n_registros*100,2) perc_conformidade,
    from viagens v
    join distancia_estimada d
    on v.data = d.data
    AND v.id_veiculo = d.id_veiculo
    AND v.servico = d.servico
    AND v.shape_id = d.shape_id
    and v.trip_number = d.trip_number
    order by data, id_veiculo, servico, shape_id, datetime_partida)
select 
    c.*
from conformidade c
inner join (
    -- Filtra viagens com maior percentual de conformidade (caso tenha
    -- match com mais de 1 shape no mesmo horario)
    select 
        id_veiculo,
        servico,
        datetime_partida,
        max(perc_conformidade) as perc_conformidade
    from conformidade
    group by 1,2,3
) p
on c.id_veiculo = p.id_veiculo
and c.servico = p.servico
and c.datetime_partida = p.datetime_partida
and c.perc_conformidade = p.perc_conformidade