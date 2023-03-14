{{
    config(
        materialized="incremental",
        partition_by={"field": "data_inicio", "data_type": "date", "granularity": "day"},
        unique_key=["tipo_viagem", "data_inicio"],
        incremental_strategy="insert_overwrite",
    )
}}

-- todo: 
--  1. adicionar start e end date
--  2. rever classificacao na sppo_veiculo_dia

-- DEC RIO N. 51940/2023
with valor_km as (
    select null as indicador_licenciado, null as indicador_ar_condicionado, null as indicador_autuacao, "Nao licenciado" as status, 0 as valor_km, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    union all
    select true as indicador_licenciado, false as indicador_ar_condicionado, null as indicador_autuacao, "Licenciado sem ar" as status, 1.97 as valor_km, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    union all
    select true as indicador_licenciado, true as indicador_ar_condicionado, null as indicador_autuacao, "Licenciado com ar e não autuado (023.II)" as status, 2.81 as valor_km, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    union all
    select true as indicador_licenciado, true as indicador_ar_condicionado, true as indicador_autuacao, "Licenciado com ar e autuado (023.II)" as status, 0 as valor_km, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
)
select * from valor_km