with sumario_viagem as (
    select
        data,
        tipo_dia,
        servico_realizado as servico,
        sentido,
        inicio_periodo,
        fim_periodo,
        count(id_viagem) as viagens_realizadas,
        sum(distancia_aferida) as distancia_total_aferida,
        max(distancia_planejada) as distancia_planejada
    from 
        {{ ref("viagem_completa") }}
    group by
        1,2,3,4,5,6
)
-- 2. Calcula viagens e distancia total por servico e período
select 
    p.consorcio,
    p.data,
    p.tipo_dia,
    p.servico,
    p.sentido,
    p.start_time as inicio_periodo,
    p.end_time as fim_periodo,
    viagens as viagens_planejadas,
    ifnull(viagens_realizadas, 0) as viagens_realizadas,
    case 
        when viagens >= ifnull(viagens_realizadas, 0)
        then ifnull(viagens_realizadas, 0)
        else viagens
    end as viagens_subsidio,
    ifnull(distancia_planejada, 0) * viagens as distancia_total_planejada,
    case 
        when viagens >= viagens_realizadas
        then ifnull(distancia_planejada, 0) * viagens_realizadas
        else ifnull(distancia_planejada, 0) * viagens
    end as distancia_total_subsidio,
    ifnull(distancia_total_aferida, 0) as distancia_total_aferida,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from (
    select
        *
    from
        {{ ref("viagem_planejada") }}
) p
left join
    sumario_viagem v
on
    v.servico = p.servico
    and v.tipo_dia = p.tipo_dia
    and v.sentido = p.sentido
    and v.data = p.data
    and v.inicio_periodo = p.start_time
    and v.fim_periodo = p.end_time