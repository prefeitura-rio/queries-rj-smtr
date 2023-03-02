{{
    config(
        materialized="incremental",
        partition_by={"field": "data_inicio", "data_type": "date", "granularity": "day"},
        unique_key=["tipo_penalidade", "data_inicio"],
        incremental_strategy="insert_overwrite",
    )
}}

-- DEC RIO N. 51940/2023
with penalidade as (
    select 0 as perc_km_inferior, 40 as perc_km_superior, "Grave" as tipo_penalidade, 1126.55 as valor, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    union all
    select 40 as perc_km_inferior, 60 as perc_km_superior, "Média" as tipo_penalidade, 563.28 as valor, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    union all
    select 60 as perc_km_inferior, 80 as perc_km_superior, "Nula" as tipo_penalidade, 0 as valor, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    union all
    select 80 as perc_km_inferior, 100 as perc_km_superior, null as tipo_penalidade, null as valor, date("2023-01-16") as data_inicio, date("2023-12-31") as data_fim
    
)
select * from penalidade