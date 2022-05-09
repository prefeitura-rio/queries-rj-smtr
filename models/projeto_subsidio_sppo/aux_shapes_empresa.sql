-- 1. Selecina shapes e adiciona coluna de tipo dia (TODO: ver ordem de
--    prioridade com RM, RT, SA, DA)
with data_efetiva as (
    select 
        data,
        data_versao_efetiva_agency,
        data_versao_efetiva_shapes
    from 
        {{ var('sigmob_data_versao') }}
    where
        data between DATE_SUB(DATE("{{ var("start_date") }}"), INTERVAL 8 DAY) and DATE("{{ var("start_date") }}")
),
shapes as (
    select 
        s.* except(data_versao),
        e.data,
        SUBSTR(shape_id, 11, 1) as sentido_shape,
        case 
            when SUBSTR(shape_id, 12, 2) = "DD" then "Domingo"
            when SUBSTR(shape_id, 12, 2) = "SS" then "Sabado"
            when SUBSTR(shape_id, 12, 2) = "DU" then "Dia Útil"
        end as tipo_dia
    from (
        select 
            * 
        from 
            {{ var('sigmob_shapes') }}
        where 
            data_versao between DATE_SUB(DATE("{{ var("start_date") }}"), INTERVAL 8 DAY)
                and DATE("{{ var("start_date") }}") -- TODO: Reduzir processamento com data_efetiva
    ) s
    inner join
        data_efetiva e
    on
        s.data_versao = e.data_versao_efetiva_shapes
    where 
        id_modal_smtr in ('22','O')
),
-- 2. Adiciona informação de consórcio
agency as (
    select 
        e.data, 
        a.agency_name as consorcio, 
        a.route_id
    from (
        select 
            * 
        from 
            {{ var("sigmob_routes") }}
        where 
            data_versao between (select min(data_versao_efetiva_agency) from data_efetiva)
                and (select max(data_versao_efetiva_agency) from data_efetiva)
    ) a
    inner join
        data_efetiva e
    on
        a.data_versao = e.data_versao_efetiva_agency
    where 
        idModalSmtr in ("22", "O")
),
shape_agency as (
    select
        s.*,
        a.consorcio
    from
        shapes s
    left join
        agency a
    on s.route_id = a.route_id
    and s.data = a.data
),
-- 2. Adiciona identificador da empresa que opera a linha do shape
shape_empresa as (
    select
        s.*,
        e.cod_empresa as id_empresa
    from shape_agency s
    left join (
        select 
            *
        from {{ var('linha_empresa') }}
    ) e
    on 
        RTRIM(TRIM(s.linha_gtfs, " "), "SANVPREDBCFG") = e.linha -- junta apenas pela linha (309SN -> 309)
),
-- 3. Filtra shapes de servicos ciculares planejados (recupera sentido dos shapes separados em ida/volta)
shape_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shape_empresa s
    left join (
        select distinct 
            servico, sentido
        from 
            {{ var("sppo_viagem_planejada") }}
        where 
            sentido = "C"
    ) c
    on 
        TRIM(s.linha_gtfs, " ") = TRIM(c.servico, " ") -- ajusta tipo de servico entre tabelas (ex: 309 SN -> 309SN)
),
-- 4. Filtra shapes de servicos não ciculares planejados
shape_nao_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shape_empresa s
    left join (
        select distinct 
            servico, sentido
        from 
            {{ var("sppo_viagem_planejada") }}
        where 
            sentido = "I" or sentido = "V"
    ) c
    on 
        TRIM(s.linha_gtfs, " ") = TRIM(c.servico, " ") -- ajusta tipo de servico entre tabelas (ex: 309 SN -> 309SN)
        and s.sentido_shape = c.sentido
),
-- 5. Junta infos de shapes ciculares e não ciculares
shape_sentido as (
    select 
        * 
    from 
        shape_circular
    union all  (
        select 
            *
        from 
            shape_nao_circular
    )
)
select
    e.*,
    s.sentido,
    concat(e.shape_id, e.id_empresa, FORMAT_DATE("%Y%d%m", e.data)) as id_shape_empresa_data,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    shape_empresa e
left join 
    (select * from shape_sentido where sentido is not null) s
on 
    e.shape_id = s.shape_id
where 
    id_empresa is not null