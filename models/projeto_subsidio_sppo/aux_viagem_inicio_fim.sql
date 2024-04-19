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
        * except(datetime_chegada, posicao_veiculo_geo),
        posicao_veiculo_geo as posicao_partida,
        lead(datetime_chegada) over(
            partition by id_veiculo, shape_id 
            order by id_veiculo, shape_id, timestamp_gps
        ) as datetime_chegada,
        lead(posicao_veiculo_geo) over(
            partition by id_veiculo, shape_id 
            order by id_veiculo, shape_id, timestamp_gps
        ) as posicao_chegada,
    from aux_inicio_fim
)
-- 4. Filtra colunas e cria campo identificador da viagem (id_viagem)
select distinct
    concat(id_veiculo, "-", servico_realizado ,"-", sentido, "-", shape_id_planejado, "-", FORMAT_DATETIME("%Y%m%d%H%M%S", datetime_partida)) as id_viagem,
    data,
    id_empresa,
    id_veiculo,
    servico_informado, -- no momento da partida
    servico_realizado,
    trip_id,
    shape_id,
    sentido_shape,
    round((st_distance(start_pt, posicao_partida) + st_distance(end_pt, posicao_chegada))/1000, 3) as distancia_inicio_fim,
    distancia_planejada,
    shape_id_planejado,
    trip_id_planejado,
    sentido,
    datetime_partida,
    datetime_chegada,
    '{{ var("version") }}' as versao_modelo
from 
    inicio_fim
where 
    datetime_partida is not null
    {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
    and extract(date from datetime_partida) = date_sub(date("{{ var("run_date") }}"), interval 1 day)
    {% endif %}