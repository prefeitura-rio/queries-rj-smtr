{{
    config(
        materialized = 'ephemeral'
    )
}}
{%if is_incremental() %}
{% set run_date = run_query('select max(data) from rj-smtr-dev.projeto_subsidio_sppo_propostas.aux_registros_status_viagem').columns[0].values()[0] %}
{% else %}
{% set run_date = '2022-03-27' %}
{% endif %}
with empresas as (
    select 
        *
    from {{ var('linha_empresa') }}
),
shapes_empresa as (
    select
        s.*,
        cod_empresa as id_empresa,
        linha as linha_empresa
    from (
        select *
        from `rj-smtr-dev.br_rj_riodejaneiro_sigmob.shapes_geom`
        where data_versao between DATE_SUB(DATE("2022-03-27"), INTERVAL 7 DAY) and DATE("2022-03-27")        and id_modal_smtr in ('22','23','O')
    ) s
    join empresas e
    on LTRIM(s.linha_gtfs, "SNVE") = e.linha
)
select * from shapes_empresa