-- 1. Sumariza viagens aferidas
with viagem as (
    select
        data,
        trip_id,
        count(id_viagem) as viagens_realizadas
    from 
        {{ ref("viagem_completa") }}
    group by
        1,2
),
-- 2. Junta informações de viagens planejadas às realizadas
planejado as (
    select distinct
        p.*,
        ifnull(v.viagens_realizadas, 0) as viagens_realizadas
    from (
        select
            consorcio,
            data,
            tipo_dia,
            trip_id_planejado as trip_id,
            servico,
            vista,
            sentido,
            inicio_periodo,
            fim_periodo,
            sum(ifnull(distancia_planejada, 0)) as distancia_planejada,
            max(viagens) as viagens_planejadas
        from
            {{ ref("viagem_planejada") }}
        where data_shape is not null
        group by 1,2,3,4,5,6,7,8,9
    ) p
    left join
        viagem v
    on
        v.trip_id = p.trip_id
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