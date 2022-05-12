-- 1. Filtra shapes a partir da data de início do subsídio
with data_efetiva as (
    select 
        data,
        data_versao_efetiva_shapes
    from 
        {{ var('sigmob_data_versao') }}
    where
        data between date_sub(date_sub(date("{{ var("run_date") }}"), interval 1 month), interval 1 day) and date_sub("{{ var("run_date") }}", interval 1 day)
),
shapes as (
    select
        data_versao,
        shape_id,
        shape,
        s.shape_distance/1000 as distancia_shape,
        start_pt,
        end_pt,
        linha_gtfs
    from {{ var('sigmob_shapes') }} s
    where
        data_versao between date_sub(date("{{ var("run_date") }}"), interval 2 month) and date_sub("{{ var("run_date") }}", interval 1 day)
        and id_modal_smtr in ('22','O')
),
-- 2. Corrije buracos com a data versao efetiva
shapes_data as (
    select 
        e.data,
        s.* except(data_versao, linha_gtfs),
        case
            -- TODO: ver ordem de prioridade com RM, RT, SA, DA
            when SUBSTR(shape_id, 12, 2) = "DD" then "Domingo"
            when SUBSTR(shape_id, 12, 2) = "SS" then "Sabado"
            when SUBSTR(shape_id, 12, 2) = "DU" then "Dia Útil"
        end as tipo_dia,
        REGEXP_REPLACE(linha_gtfs, " ", "") as servico, -- 309 SN -> 309SN
        REGEXP_REPLACE(linha_gtfs, "(S|A|N|V|P|R|E|D|B|C|F|G| )", "") as linha, -- 309SN -> 309
        SUBSTR(shape_id, 11, 1) as sentido_shape
    from 
        data_efetiva e
    left join
        shapes s
    on
        s.data_versao = e.data_versao_efetiva_shapes
)
select
    *
from
    shapes_data
where 
    tipo_dia is not null