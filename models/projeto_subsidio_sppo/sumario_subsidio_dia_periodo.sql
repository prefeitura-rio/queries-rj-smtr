-- 1. Sumariza viagens aferidas
with viagem as (
    select
        data,
        servico_informado,
        shape_id,
        count(id_viagem) as viagens_realizadas
    from 
        {{ ref("viagem_completa") }}
    group by
        1,2,3
),
-- 2. Junta informações de viagens planejadas às realizadas
planejado as (
    select 
        p.consorcio,
        p.data,
        p.tipo_dia,
        p.servico,
        p.vista,
        p.sentido,
        p.inicio_periodo,
        p.fim_periodo,
        p.viagens as viagens_planejadas,
        ifnull(v.viagens_realizadas, 0) as viagens_realizadas,
        ifnull(p.distancia_planejada, 0) as distancia_planejada
    from (
        select distinct
            *
        from
            {{ ref("viagem_planejada") }}
        where data_shape is not null
    ) p
    left join
        viagem v
    on
        v.shape_id = p.shape_id
        and v.servico_informado = p.servico
        and v.data = p.data
),
-- 3. Limita máximo de viagens do subsídio = total planejado 
viagem_subsidio as (
    select
        *,
        viagens_realizadas as viagens_subsidio
        -- case 
        --     when viagens_realizadas > viagens_planejadas
        --     then viagens_planejadas
        --     else viagens_realizadas
        -- end as viagens_subsidio
    from planejado
)
-- 4 . Adiciona informações de distância total
select 
    * except(distancia_planejada),
    round(viagens_planejadas * distancia_planejada, 3) as distancia_total_planejada,
    round(viagens_subsidio * distancia_planejada, 3) as distancia_total_subsidio,
    round(viagens_realizadas * distancia_planejada, 3) as distancia_total_aferida,
    '{{ var("version") }}' as versao_modelo
from viagem_subsidio