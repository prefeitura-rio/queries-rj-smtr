-- 1. Filtra tabela de shapes até o mês anterior para limitar processamento
with shapes as (
    select
        data_versao,
        shape_id,
        shape,
        s.shape_distance/1000 as distancia_planejada,
        start_pt,
        end_pt,
        linha_gtfs
    from {{ var('sigmob_shapes') }} s
    where
        data between date_sub(date("{{ var("run_date") }}"), interval 1 month) and date("{{ var("run_date") }}")
        and id_modal_smtr in ('22','O')
),
-- 2. Adiciona data efetiva dos shapes - garante a última versão
--    caso haja falha de captura no dia
shapes_efetiva as (
    select 
        e.data,
        s.* except(data_versao)
    from (
        select 
            data,
            data_versao_efetiva_shapes
        from 
            {{ var('sigmob_data_versao') }}
        where
            data between date_sub(date("{{ var("run_date") }}"), interval 2 month) and date("{{ var("run_date") }}")
    ) e
    left join
        shapes s
    on
        s.data_versao = e.data_versao_efetiva_shapes
),
-- 3. Cria coluna de serviço e linha padrões com base na linha_gtfs,
--    adiciona classificacao de tipo dia
shapes_servico as (
    select
        * except(linha_gtfs),
        case
            -- TODO: ver ordem de prioridade com RM, RT, SA, DA
            when SUBSTR(shape_id, 12, 2) = "DD" then "Domingo"
            when SUBSTR(shape_id, 12, 2) = "SS" then "Sabado"
            when SUBSTR(shape_id, 12, 2) = "DU" then "Dia Útil"
        end as tipo_dia,
        REGEXP_REPLACE(linha_gtfs, " ", "") as servico, -- 309 SN -> 309SN
        -- REGEXP_REPLACE(linha_gtfs, "(S|A|N|V|P|R|E|D|B|C|F|G| )", "") as linha, -- 309SN -> 309
        SUBSTR(shape_id, 11, 1) as sentido_shape
    from
        shapes_data
    where 
        tipo_dia is not null
)
-- 4. Filtra shapes de servicos circulares planejados (recupera
--    sentido dos shapes separados em ida/volta)
with shape_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shapes_servico s
    inner join (
        select 
            tipo_dia,
            servico,
            sentido
        from 
            {{ var("aux_viagem_planejada") }}
        where
            sentido = "C"
    ) c
    on 
        s.servico = c.servico
        and s.tipo_dia = c.tipo_dia
),
-- 5. Filtra shapes de servicos não circulares planejados
shape_nao_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shapes_servico
    inner join (
        select 
            tipo_dia,
            servico,
            sentido
        from 
            {{ var("aux_viagem_planejada") }}
        where 
            sentido = "I" or sentido = "V"
    ) c
    on 
        s.servico = c.servico
        and s.sentido_shape = c.sentido
        and s.tipo_dia = c.tipo_dia
),
-- 6. Junta infos de shapes circulares e não ciculares
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
-- 7. Adiciona distância total planejada da viagem circular no shape de
--    ida (usaremos apenas a ida como padrão, juntando nela as demais infos da volta
--    consecutiva)
select
    e.*,
    s.sentido,
    case when
        sentido = "C" and sentido_shape = "I"
        then distancia_planejada + lead(distancia_planejada) over (
                partition by data, servico, tipo_dia 
                order by data, servico, tipo_dia, sentido_shape)
        else distancia_planejada
    end  as distancia_planejada
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    shapes_servico e
inner join 
    shape_sentido s
on 
    e.shape_id = s.shape_id