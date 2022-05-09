
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
        {{ ref('aux_registros_status_trajeto') }}
    where 
        shape_id is not null
    order by 
        shape_id, timestamp_gps
),
-- 2. Classifica início-fim consecutivos como partida-chegada da viagem
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
-- 3. Junta partida-chegada da viagem na mesma linha
inicio_fim AS (
    select 
        * except(datetime_chegada), 
        lead(datetime_chegada) over(
            partition by id_veiculo, shape_id 
            order by id_veiculo, shape_id, timestamp_gps
        ) as datetime_chegada,
    from aux_inicio_fim
),
-- 4. Filtra colunas e cria campo identificador da viagem (id_viagem)
viagem as (
    select distinct
        data,
        tipo_dia,
        consorcio,
        id_veiculo,
        id_empresa,
        servico_informado, -- no momento da partida
        servico_realizado,
        shape_id,
        sentido_shape,
        sentido,
        concat(id_veiculo, shape_id, FORMAT_DATETIME("%Y%d%m%H%M%S", datetime_partida)) as id_viagem,
        datetime_partida,
        datetime_chegada
    from 
        inicio_fim
    where 
        datetime_partida is not null
)
select
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem