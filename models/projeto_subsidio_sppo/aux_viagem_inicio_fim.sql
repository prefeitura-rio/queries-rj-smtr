
-- 1. Cria colunas identificadoras de início (starts) e fim (ends) de viagens
with aux_status as (
    select 
        *,
        string_agg(status_viagem,"") over (
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between current row and 1 following) = 'startmiddle' starts,
        string_agg(status_viagem,"") over (
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between 1 preceding and current row) = 'middleend' ends
    from 
        {{ ref('aux_registros_status_viagem') }}
    where 
        shape_id is not null
    order by 
        shape_id, timestamp_gps
),
-- 2. Filtra início/fim e classifica datetime da partida/chegada da viagem
aux_inicio_fim AS (
    select 
        *,
        case 
            when
            string_agg(status_viagem,"") over (
                partition by id_veiculo, shape_id
                order by id_veiculo, shape_id, timestamp_gps
                rows between CURRENT row and 1 following) = 'startend' 
            then timestamp_gps 
        end datetime_partida,
        case 
            when string_agg(status_viagem,"") over (
                partition by id_veiculo, shape_id
                order by id_veiculo, shape_id, timestamp_gps
                rows between 1 preceding and CURRENT row) = 'startend' 
            then timestamp_gps
        end datetime_chegada
    from 
        aux_status
    where 
        starts = true OR ends = true
),
inicio_fim AS (
    select 
        * except(datetime_chegada), 
        lead(datetime_chegada) over(
            partition by id_veiculo, shape_id 
            order by id_veiculo, shape_id, timestamp_gps
        ) as datetime_chegada,
    from aux_inicio_fim
),
-- 3. Filtra viagem de menor tempo identificada para cada
--    datetime_partida, shape_id e id_veiculo
aux_filtrada as (
    select 
        *,
        row_number() over (
            partition by data, id_veiculo, shape_id, datetime_partida
            order by tempo_viagem
        ) as rn
    from (
        select 
            data,
            id_veiculo,
            id_empresa,
            servico_informado,
            servico_realizado,
            shape_id,
            datetime_partida,
            datetime_chegada,
            datetime_diff(datetime_chegada, datetime_partida, minute) as tempo_viagem,
        from 
            inicio_fim
        where 
            datetime_partida is not null
    ) 
),
filtrada as (
    select 
        r.* except(rn),
    from 
        aux_filtrada r
    where 
        rn = 1
    order by 
        datetime_partida
)
-- 4. Adiciona colunas auxiliares
select
    data,
    case
        when extract(dayofweek from datetime_partida) = 1 then 'Domingo'
        when extract(dayofweek from datetime_partida) = 7 then 'Sabado'
        -- when data = data_feriado then 'Feriado'
        else 'Dia Útil'
    end tipo_dia,
    id_veiculo,
    id_empresa,
    servico_informado,
    servico_realizado,
    shape_id,
    SUBSTR(shape_id, 11, 1) as sentido,
    row_number() over (
            partition by id_veiculo, shape_id
            order by datetime_partida
    ) as ordem_viagem,
    datetime_partida,
    datetime_chegada,
    tempo_viagem,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    filtrada