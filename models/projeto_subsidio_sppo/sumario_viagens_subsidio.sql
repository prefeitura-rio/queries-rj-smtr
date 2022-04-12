select 
    consorcio,
    data, 
    tipo_dia,
    servico, 
    sentido,
    inicio_periodo, 
    fim_periodo,
    viagens_teorico,
    count(sentido) as viagens_realizadas,
    max(distancia_teorica) as distancia_teorica,
    round(max(distancia_teorica)*count(trip_number),2) as distancia_teorica_realizada,
    round(sum(distancia_km),2) as distancia_estimada_realizada
from `rj-smtr-dev.projeto_subsidio_sppo_v2.viagens_validas_periodo`
where (perc_conformidade_distancia >= 80 and perc_conformidade_distancia <= 120)
and (perc_conformidade_shape >= 80 and perc_conformidade_shape <= 120)
group by 1,2,3,4,5,6,7,8;