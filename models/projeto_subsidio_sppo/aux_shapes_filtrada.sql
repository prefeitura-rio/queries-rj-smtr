-- 1. Define a data e tipo dia do período avaliado (D-3, D-2)
with data_efetiva as (
    select distinct
        data,
        data_versao_efetiva_shapes,
        case
            when extract(dayofweek from data) = 1 then 'Domingo'
            when extract(dayofweek from data) = 7 then 'Sabado'
            else 'Dia Útil'
        end as tipo_dia
    from 
        {{ var('sigmob_data_versao') }} a
    where
        data between date_sub("{{ var("run_date") }}", interval 3 day)
            and date_sub("{{ var("run_date") }}", interval 2 day)
),
-- 2. Filtra tabela de shapes para limitar processamento (até 14 dias antes de D-3)
shapes as (
    select
        data_versao,
        trip_id,
        shape_id,
        shape,
        s.shape_distance/1000 as distancia_planejada,
        start_pt,
        end_pt,
        linha_gtfs
    from {{ var('sigmob_shapes') }} s
    where
        data_versao between date_sub(date("{{ var("run_date") }}"), interval 17 day) and date("{{ var("run_date") }}")
        and id_modal_smtr in ('22','O')
),
-- 3. Adiciona data efetiva dos shapes - garante a última versão
--    caso haja falha de captura no dia
shapes_efetiva as (
    select 
        e.data,
        e.tipo_dia,
        SUBSTR(shape_id, 12, 2) as variacao_itinerario,
        linha_gtfs as servico,  -- ex: 309SN
        SUBSTR(shape_id, 11, 1) as sentido_shape,
        s.data_versao as data_shape,
        s.* except(data_versao, linha_gtfs)
    from 
        data_efetiva e
    left join
        shapes s
    on
        s.data_versao = e.data_versao_efetiva_shapes
),
-- 4. Filtra shapes de servicos circulares planejados (recupera
--    sentido dos shapes separados em ida/volta)
shape_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shapes_efetiva s
    inner join (
        select 
            variacao_itinerario,
            servico,
            sentido
        from 
            {{ ref("aux_viagem_planejada") }}
        where
            sentido = "C"
             -- TODO: remover filtro após mudança no quadro planejado
            and variacao_itinerario in ("DU", "SS", "DD")
    ) c
    on
        s.servico = c.servico
        and s.variacao_itinerario = c.variacao_itinerario
),
-- 5. Filtra shapes de servicos não circulares planejados
shape_nao_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shapes_efetiva s
    inner join (
        select 
            variacao_itinerario,
            servico,
            sentido
        from 
            {{ ref("aux_viagem_planejada") }}
        where 
            sentido = "I" or sentido = "V"
            -- TODO: remover filtro após mudança no quadro planejado
            and variacao_itinerario in ("DU", "SS", "DD")
    ) c
    on 
        s.servico = c.servico
        and s.sentido_shape = c.sentido
        and s.variacao_itinerario = c.variacao_itinerario
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
    e.* except(distancia_planejada),
    s.sentido,
    case when
        sentido = "C" and sentido_shape = "I"
        then distancia_planejada + lead(distancia_planejada) over (
                partition by data, servico, variacao_itinerario 
                order by data, servico, variacao_itinerario, sentido_shape)
        else distancia_planejada
    end as distancia_planejada,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    shapes_efetiva e
inner join 
    shape_sentido s
on 
    e.shape_id = s.shape_id