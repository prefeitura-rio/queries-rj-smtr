{{ config(
    materialized='incremental',
          partition_by={
            "field":"data_versao",
            "data_type": "date",
            "granularity":"day"
      }
)
}}
-- ATUALIZADA A CADA 15 DIAS
-- 1. Cria view das trips consideradas no subsidio
with content as (
    select
    trip_id,
    concat(substr(trip_id, 1, 10), substr(trip_id, 12, 4)) as trip_no_direction,
    json_value(content, "$.route_id") route_id,
    json_value(content, "$.service_id") service_id,
    json_value(content, "$.trip_headsign") trip_headsign,
    json_value(content, "$.trip_short_name") trip_short_name,
    json_value(content, "$.direction_id") direction_id,
    json_value(content, "$.block_id") block_id,
    json_value(content, "$.shape_id") shape_id,
    json_value(content, "$.variacao_itinerario") variacao_itinerario,
    json_value(content, "$.versao") versao,
    json_value(content, "$.complemento") complemento,
    json_value(content, "$.via") via,
    json_value(content, "$.observacoes") observacoes,
    json_value(content, "$.ultima_medicao_operante") ultima_medicao_operante,
    json_value(content, "$.idModalSmtr") id_modal_smtr,
    json_value(content, "$.Direcao") direcao,
    json_value(content, "$.id") id,
    DATE(data_versao) data_versao
    from `rj-smtr.br_rj_riodejaneiro_sigmob.trips` -- {{ ref("trips") }}
    where data_versao = "{{var('versao_fixa_sigmob')}}"
)
select distinct
    case 
        when REGEXP_EXTRACT(c.trip_short_name, r'[A-Z]+') is null
        then trip_short_name
        else concat(REGEXP_EXTRACT(c.trip_short_name, r'[A-Z]+'), 
                REGEXP_EXTRACT(c.trip_short_name, r'[0-9]+')) 
    end as trip_short_name,
    q.vista as trip_headsign,
    c.* except(trip_no_direction, trip_short_name, trip_headsign)
from
    content c
inner join (
    select
        trip_id,
        vista,
        concat(substr(trip_id, 1, 10), substr(trip_id, 12, 4)) as trip_no_direction,
    from
        {{ var("quadro_horario") }}
    ) q
on c.trip_no_direction = q.trip_no_direction
-- remove trips da 410 duplicadas na 412:
where trip_short_name != "412"