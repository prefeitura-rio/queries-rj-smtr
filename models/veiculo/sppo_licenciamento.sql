{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    -- Tabela de licenciamento
    stu as (
        select *
        from {{ ref("sppo_licenciamento_stu") }} as t
        {% if var("stu_data_versao") != "" -%}
        where data = date("{{ var('stu_data_versao') }}")
        {% else %}
        where data = date_add(date("{{ var('run_date') }}"), interval 5 day)
        {%- endif %}
        AND tipo_veiculo NOT LIKE "%ROD%"
    )--,
    -- Solicitações válidas de licenciamento
    -- TODO: add if run_date < ... antes de subir p proda te data max de validade das
    -- solicitacoes
    -- solicitacao as (
    --     select * except (solicitacao)
    --     from {{ ref("sppo_licenciamento_solicitacao") }} as t
    --     where
    --         data = date("{{ var('sppo_licenciamento_solicitacao_data_versao') }}")  -- fixo
    --         and status = "Válido"
    --         and solicitacao != "Baixa"
    --         AND tipo_veiculo NOT LIKE "%ROD%"
    -- )
-- select 
--     {% if var("stu_data_versao") != "" -%}
--     date("{{ var('stu_data_versao') }}") as data,
--     {% else %}
--     date_add(date("{{ var('run_date') }}"), interval 5 day) as data,
--     {%- endif %}
--     * except (data, timestamp_captura)
-- from solicitacao sol
-- union all
-- Se tiver id_veiculo em solicitacao e for valido, substitui o que esta em
-- licenciamento
-- (
    select stu.* except (timestamp_captura)
    from stu
    -- left join solicitacao sol on stu.id_veiculo = sol.id_veiculo
    -- where sol.id_veiculo is null
-- )
