{% set start_date = "2022-03-27" %}

-- 1. Seleciona sinais de GPS registrados no período
with gps as (
    select 
        g.* except(longitude, latitude),
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        d.data_versao_efetiva_shapes data_versao
    from 
        {{ var("sppo_gps") }} g
    join 
        {{ var("sigmob_data_versao") }} d
    on 
        g.data = d.data
    where 
        g.data between DATE_SUB(DATE("{{start_date}}"), INTERVAL 7 DAY) and DATE("{{start_date}}")
),
-- 2. Seleciona shapes atualizados do período
shapes as (
    select 
        *
    from 
        {{ var("sigmob_shapes") }}
    where 
        data_versao between 
            (select min(data_versao) from gps) 
            and (select max(data_versao) from gps)
        and id_modal_smtr in ('22', '23', 'O')
),
-- 3. Classifica a posição do veículo em todos os shapes possíveis do
--    mesmo servico
status_viagem as (
    select
        id_veiculo,
        timestamp_gps,
        data,
        servico,
        distancia,
        shape_id,
        shape_distance,
        flag_trajeto_correto,
        case
            when ST_DWITHIN(posicao_veiculo_geo, start_pt, 200)
            then 'start'
            when ST_DWITHIN(posicao_veiculo_geo, end_pt, 200)
            then 'end'
            when flag_trajeto_correto is true
            then 'middle'
        else 'out'
        end status_viagem
    from 
        gps g
    join 
        shapes s
    on 
        g.data_versao = s.data_versao
        and g.servico = s.linha_gtfs
)
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    status_viagem