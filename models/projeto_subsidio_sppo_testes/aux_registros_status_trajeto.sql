-- 1. Seleciona sinais de GPS registrados no período
with gps as (
    select 
        g.* except(longitude, latitude),
        substr(id_veiculo, 2, 3) as id_empresa,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        d.data_versao_efetiva_shapes data_versao
    from 
        {{ var('sppo_gps') }} g
    join 
        {{ var('sigmob_data_versao') }} d
    on 
        g.data = d.data
    where
        g.data between DATE_SUB(DATE("{{ var("start_date") }}"), INTERVAL 8 DAY) and DATE("{{ var("start_date") }}")
),
-- 2. Seleciona shapes atualizados do período
shapes as (
    select 
        *
    from 
        {{ ref('aux_shapes_empresa') }}
    where 
        data_versao between 
            (select min(data_versao) from gps)
            and (select max(data_versao) from gps)
        and id_modal_smtr in ('22', '23', 'O')
),
-- 3. Classifica a posição do veículo em todos os shapes possíveis de
--    serviços de uma mesma empresa
status_viagem as (
    select
        data,
        id_veiculo,
        g.id_empresa,
        timestamp_gps,
        timestamp_trunc(timestamp_gps, minute) as timestamp_minuto_gps,
        posicao_veiculo_geo,
        servico as servico_informado,
        linha_gtfs as servico_realizado,
        shape_id,
        round(shape_distance/1000, 2) as distancia_teorica,
        distancia,
        case
            when ST_DWITHIN(posicao_veiculo_geo, start_pt, 100)
            then 'start'
            when ST_DWITHIN(posicao_veiculo_geo, end_pt, 100)
            then 'end'
            when ST_DWITHIN(posicao_veiculo_geo, shape, 100)
            then 'middle'
        else 'out'
        end status_viagem
    from 
        gps g
    left join 
        shapes s
    on 
        g.data_versao = s.data_versao
        and g.id_empresa = s.id_empresa
)
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    status_viagem
order by id_veiculo, timestamp_gps, shape_id