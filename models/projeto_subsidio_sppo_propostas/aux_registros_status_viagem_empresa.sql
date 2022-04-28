

-- 1. Seleciona sinais de GPS registrados no período
{{
    config(
        materialized = 'table'
    )
}}
{%if is_incremental() %}
{% set run_date = run_query('select max(data) from rj-smtr-dev.projeto_subsidio_sppo_propostas.aux_registros_status_viagem').columns[0].values()[0] %}
{% else %}
{% set run_date = '2022-03-27' %}
{% endif %}
with gps as (
    select 
        g.* except(longitude, latitude),
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        d.data_versao_efetiva_shapes data_versao
    from 
        {{ var('sppo_gps') }} g
    join 
        {{ var('sigmob_data_versao') }} d
    on 
        g.data = d.data
    where 
        g.data between DATE_SUB(DATE("{{run_date}}"), INTERVAL 7 DAY) and DATE("{{run_date}}")
),
gps_empresa as (
      select
            g.*,
            cod_empresa id_empresa
      from gps g
      join {{var('linha_empresa')}} e
      on LTRIM(g.servico, 'SVNE') = e.linha
),
-- 2. Seleciona shapes atualizados do período
shapes as (
    select 
        *
    from 
        {{var('aux_shapes_empresa')}}
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
            when ST_DWITHIN(posicao_veiculo_geo, shape, 200)
            then 'middle'
        else 'out'
        end status_viagem
    from 
        gps_empresa g
    join 
        shapes s
    on 
        g.data_versao = s.data_versao
        and g.id_empresa = s.id_empresa
)
-- select id_veiculo, count(distinct shape_id) ct_shapes_cruzados
-- from status_viagem
-- group by 1
-- order by 2 desc
select 
    *,
    'projeto_subsidio_sppo_v1.0.0' as versao_modelo
from 
    status_viagem
order by id_veiculo, timestamp_gps, shape_id