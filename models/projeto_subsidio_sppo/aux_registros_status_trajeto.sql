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
        end tipo_dia
    from 
        {{ var('sppo_gps') }} g
    where g.data between DATE_SUB(DATE("{{ var("start_date") }}"), INTERVAL 8 DAY) and DATE("{{ var("start_date") }}")
),
-- 2. Classifica a posição do veículo em todos os shapes possíveis de
--    serviços de uma mesma empresa
status_viagem as (
    select
        g.data,
        g.tipo_dia,
        g.id_veiculo,
        s.consorcio,
        s.id_empresa,
        g.timestamp_gps,
        timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
        g.posicao_veiculo_geo,
        TRIM(g.servico, " ") as servico_informado,
        TRIM(s.linha_gtfs, " ") as servico_realizado,
        s.shape_id,
        s.sentido_shape,
        s.sentido,
        round(s.shape_distance/1000, 2) as distancia_teorica,
        ifnull(g.distancia,0) as distancia, -- TODO: Checar 2% de nulos para abril
        case
            when ST_DWITHIN(g.posicao_veiculo_geo, start_pt, 100)
            then 'start'
            when ST_DWITHIN(g.posicao_veiculo_geo, end_pt, 100)
            then 'end'
            when ST_DWITHIN(g.posicao_veiculo_geo, shape, 100)
            then 'middle'
        else 'out'
        end status_viagem
    from 
        gps g
    left join 
        {{ ref('aux_shapes_empresa') }} s
    on 
        g.data = s.data
        and g.id_empresa = s.id_empresa
)
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    status_viagem
where id_empresa is not null -- TODO: Checar veículos cuja empresa não está registrada