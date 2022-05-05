{% set start_date = "2022-03-27" %}

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
        where 
            data_versao between DATE_SUB(DATE("{{start_date}}"), INTERVAL 7 DAY) and DATE("{{start_date}}")
            and id_modal_smtr in ('22','23','O')
    ) s
    left join 
        empresas e
    on 
        LTRIM(s.linha_gtfs, "SNVE") = e.linha
)
select 
    *
from shapes_empresa