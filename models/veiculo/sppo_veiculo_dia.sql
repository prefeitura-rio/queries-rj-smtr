{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    licenciamento as (
        select
            date("{{ var('run_date') }}") as data,
            id_veiculo,
            placa,
            tipo_veiculo,
            indicador_ar_condicionado
        -- status
        from {{ ref("sppo_licenciamento") }}
        -- TODO: usar redis p controle de versao
        {% if var("stu_data_versao") != "" -%}
            where data = date("{{ var('stu_data_versao') }}")
        {% else %}
            where data = date_add(date("{{ var('run_date') }}"), interval 5 day)
        {%- endif %}
    ),
    gps as (
        select distinct data, id_veiculo, true as indicador_em_operacao
        from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`  -- {{ ref("gps_sppo") }})
        where data = date("{{ var('run_date') }}")
    ),
    -- 2. Recupera dados de autuacao por operacao sem ar (Podem existir veiculos com
    -- multiplas autuacoes no dia)
    infracao as (
        select distinct data_infracao as data, placa, true as indicador_autuacao
        from {{ ref("sppo_infracao") }}
        where
            data = date_add(date("{{ var('run_date') }}"), interval 5 day)
            and data_infracao = date("{{ var('run_date') }}")
            and modo = "ONIBUS"
            and id_infracao = "023.II"
    )
select
    coalesce(l.data, g.data, i.data) as data,
    coalesce(l.id_veiculo, g.id_veiculo) as id_veiculo,
    -- 2.1. Status dos veiculos ativos no dia
    struct(
        i.indicador_autuacao, l.indicador_ar_condicionado, g.indicador_em_operacao
    ) as indicadores,
    case
        when l.id_veiculo is null
        then "Nao licenciado"
        when l.indicador_ar_condicionado = false
        then "Licenciado sem ar"
        when l.indicador_ar_condicionado = true and i.indicador_autuacao is null
        then "Licenciado com ar e não autuado (023.II)"
        when l.indicador_ar_condicionado = true and i.indicador_autuacao = true
        then "Licenciado com ar e autuado (023.II)"
    end as status
from gps g
left join licenciamento l on g.data = l.data and g.id_veiculo = l.id_veiculo
left join infracao i on g.data = i.data and l.placa = i.placa
