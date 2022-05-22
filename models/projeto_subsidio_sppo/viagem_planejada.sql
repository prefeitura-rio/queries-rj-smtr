-- 1. Recupera informações de consórcios para datas
with data_efetiva as (
    select distinct
        data,
        data_versao_efetiva_agency
    from 
        {{ var('sigmob_data_versao') }}
    where
        data between date_sub(date("{{ var("run_date") }}"), interval 1 month) and date("{{ var("run_date") }}")
),
agency as (
    select
        e.data, 
        case
            when extract(dayofweek from e.data) = 1 then 'Domingo'
            when extract(dayofweek from e.data) = 7 then 'Sabado'
            else 'Dia Útil'
        end as tipo_dia,
        a.agency_name as consorcio, 
        REGEXP_REPLACE(a.route_short_name, " ", "") as servico, -- 309 SN -> 309SN
    from {{ var("sigmob_routes") }} a
    inner join
        data_efetiva e
    on
        a.data_versao = e.data_versao_efetiva_agency
    where 
        idModalSmtr in ("22", "O")
)
-- 2. Junta infos de distancia planejada dos shapes
distancia as (
    select
        data,
        tipo_dia,
        servico,
        distancia_planejada
    from
        {{ ref("aux_shapes_filtrada") }} v
    where
        (sentido = "I" or sentido = "V")
        or (sentido = "C" and sentido_shape = "I")
)
-- 2. Junta consórcios e distancia shape aos servicos planejados
select 
    a.data,
    a.consorcio,
    v.*
from 
     {{ var("aux_viagem_planejada") }} v
left join (
    select
        d.data,
        a.consorcio,
        d.servico,
        d.distancia_planejada
    from
        distancia d
    left join
        agency a
    on
        d.servico = a.servico
        and d.data = a.data
        and d.tipo_dia = a.tipo_dia
) a
on v.servico = a.servico
and v.tipo_dia = a.tipo_dia