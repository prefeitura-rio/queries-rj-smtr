-- ATUALIZADA A CADA 15 DIAS

-- 1. Define datas do período planejado
with data_efetiva as (
    select 
        data,
        tipo_dia,
        data_versao_shapes
    from {{ ref("subsidio_data_versao_efetiva") }}
    where data_versao_shapes is not null
),
-- 2. Puxa dados de shapes usando a versão fixa do sigmob. Reconstrói
--    trip_id e shape_id de viagens circulares para cruzar com o quadro
--    horário planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob). Ajusta distância do shape para km.
shapes as (
    select
        d.* except(data_versao_shapes),
        data_versao as data_shape,
        trip_id,
        trip_id_planejado,
        shape_id,
        shape_id_planejado,
        shape,
        sentido_shape,
        round(s.shape_distance/1000, 3) as distancia_planejada,
        start_pt,
        end_pt
    from 
        data_efetiva d
    left join
        {{ ref('subsidio_shapes_geom') }} s
    on s.data_versao = d.data_versao_shapes
),
-- 3. Puxa dados de viagens planejadas no quadro horário
planejada as (
    select
        e.* except(data_versao_shapes),
        p.* except(tipo_dia)
    from 
        data_efetiva e
    left join
        {{ var("quadro_horario") }} p
    on
        e.tipo_dia = p.tipo_dia
)
-- 4. Junta shapes aos servicos planejados no quadro horário
select 
    p.* except(trip_id),
    d.* except(data, tipo_dia)
from
    planejada p
left join
    shapes d
on p.trip_id = d.trip_id_planejado
and p.data = d.data