-- Todas as viagens classificadas
-- tipo_viagem: 
-- "Completa linha correta" => circular, nao_circular OK
-- "Completa linha incorreta" => ?

{% set start_date = "2022-03-27" %}

-- 1. Seleciona viagens completas
with viagem as (
    select 
        *
    from {{ ref("viagem_circular_completa") }}
    union all (
        select * 
        from {{ ref("viagem_nao_circular_completa") }}
    )
),
-- 2. Adiciona informação de consórcio
agency as (
    select 
        data_versao, 
        agency_name as consorcio, 
        route_short_name as servico, 
    from 
        {{ var("sigmob_routes") }}
    where 
        idModalSmtr in ("22", "O")
    and 
        data_versao between DATE_SUB(DATE("{{start_date}}"), INTERVAL 7 DAY) and DATE("{{start_date}}")
)
select 
    concat(
        id_veiculo, v.servico_realizado, format_timestamp("%Y%m%d%H%M%S", datetime_partida)
    ) as id_viagem,
    data,
    tipo_dia,
    consorcio,
    id_empresa,
    id_veiculo,
    servico_informado,
    servico_realizado,
    shape_id,
    sentido,
    datetime_partida,
    datetime_chegada,
    case 
        when servico_realizado = servico_informado
        then "Completa linha correta"
        else "Completa linha incorreta"
        end as tipo_viagem,
    tempo_viagem,
    distancia_teorica,
    distancia_aferida,
    n_registros_shape,
    n_registros,
    perc_conformidade_shape,
    perc_conformidade_distancia,
    perc_conformidade_registros,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem v
left join 
    agency a
on
    v.servico_realizado = a.servico
    and v.data = a.data_versao