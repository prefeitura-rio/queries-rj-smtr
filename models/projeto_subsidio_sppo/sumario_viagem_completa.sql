select 
    consorcio,
    data, 
    tipo_dia,
    servico_realizado as servico,
    sentido,
    inicio_periodo, 
    fim_periodo,
    viagens_planejadas,
    count(sentido) as viagens_realizadas,
    case 
        when viagens_planejadas >= count(sentido)
        then count(sentido)
        else viagens_planejadas
    end as viagens_subsidio,
    round(max(distancia_teorica)*viagens_planejadas, 2) as distancia_total_planejada,
    case 
        when viagens_planejadas >= count(sentido)
        then round(max(distancia_teorica)*count(sentido), 2)
        else round(max(distancia_teorica)*viagens_planejadas, 2)
    end as distancia_total_subsidio,
    round(sum(distancia_aferida), 2) as distancia_total_aferida,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    {{ ref("viagem_completa") }}
where 
    viagens_planejadas is not null
    and tipo_viagem = "Completa linha correta"
group by 
    1,2,3,4,5,6,7,8