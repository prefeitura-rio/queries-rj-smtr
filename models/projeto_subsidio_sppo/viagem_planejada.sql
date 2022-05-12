-- TODO: Passar para sumario_viagem_completa
{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       }
)
}}

-- 1. Ajusta servico e adiciona coluna de linha
with viagem_planejada as (
    select 
        * except(servico),
        REGEXP_REPLACE(servico, " ", "") as servico,
        REGEXP_REPLACE(servico, "(S|A|N|V|P|R|E|D|B|C|F|G| )", "") as linha
    from 
        {{ var("aux_viagem_planejada") }}
),
-- 2. Cruza informação de consórcio e replica viagens planejadas para os
--    dias avaliados no período
data_efetiva as (
    select distinct
        data,
        data_versao_efetiva_agency
    from 
        {{ var('sigmob_data_versao') }}
    where
        data between date_sub(date("{{ var("run_date") }}"), interval 1 month) and date_sub("{{ var("run_date") }}", interval 1 day)
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
select 
    a.data,
    a.consorcio,
    v.*
from 
    viagem_planejada v
left join 
    agency a
on v.servico = a.servico
and v.tipo_dia = a.tipo_dia