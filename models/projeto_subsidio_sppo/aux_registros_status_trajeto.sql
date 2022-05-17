-- 1. Seleciona sinais de GPS registrados no período
with gps as (
    select 
        g.* except(longitude, latitude),
        substr(id_veiculo, 2, 3) as id_empresa,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        case
            when extract(dayofweek from timestamp_gps) = 1 then 'Domingo'
            when extract(dayofweek from timestamp_gps) = 7 then 'Sabado'
            else 'Dia Útil'
        end as tipo_dia
    from 
        {{ var('gps_sppo') }} g
    where g.data between date_sub(date("{{ var("run_date") }}"), interval 1 month) and date_sub("{{ var("run_date") }}", interval 1 day)
),
-- 2. Classifica a posição do veículo em todos os shapes possíveis de
--    serviços de uma mesma empresa
status_viagem as (
    select
        g.data,
        g.tipo_dia,
        g.id_veiculo,
        g.id_empresa,
        g.timestamp_gps,
        timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
        g.posicao_veiculo_geo,
        TRIM(g.servico, " ") as servico_informado,
        s.servico as servico_realizado,
        s.shape_id,
        s.sentido_shape,
        s.sentido,
        s.distancia_shape,
        ifnull(g.distancia,0) as distancia,
        case
            when ST_DWITHIN(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }})
            then 'start'
            when ST_DWITHIN(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }})
            then 'end'
            when ST_DWITHIN(g.posicao_veiculo_geo, shape, {{ var("buffer") }})
            then 'middle'
        else 'out'
        end status_viagem
    from 
        gps g
    inner join 
        {{ ref('aux_shapes_empresa') }} s
    on 
        g.data = s.data
        -- and g.id_empresa = s.id_empresa
        and g.servico = s.servico
)
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    status_viagem