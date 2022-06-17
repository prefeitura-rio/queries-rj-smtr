-- 1. Calcula a distância total percorrida por viagem, separada por
--    shape. No caso de viagen circulares, soma distancia de ida e volta
--    de cada uma de forma independente.
with distancia as (
    select distinct
        id_viagem,
        sentido_shape,
        sum(distancia)/1000 as distancia_aferida,
        sum(case when status_viagem = "middle" then 1 else 0 end) as n_registros_middle,
        sum(case when status_viagem = "start" then 1 else 0 end) as n_registros_start,
        sum(case when status_viagem = "end" then 1 else 0 end) as n_registros_end,
        sum(case when status_viagem = "out" then 1 else 0 end) as n_registros_out,
        count(timestamp_gps) as n_registros_total,
        count(distinct timestamp_minuto_gps) as n_registros_minuto
    from (
        select distinct * except(posicao_veiculo_geo, start_pt, end_pt)
        from {{ ref("registros_status_viagem") }}
    )
    group by 1,2
),
-- 2. Adiciona distância do 1o/último sinal de gps ao início/final do
--    shape. Isso é necessário pois o 1o/ultimo sinal é contabilizado
--    apenas quando o veiculo sai/chega dentro do raio de 500m ao redor
--    do ponto inicial/final.
inicio_fim as (
    select distinct
        d.* except(distancia_aferida),
        round(distancia_aferida + distancia_inicio_fim, 3) as distancia_aferida,
        n_registros_middle + n_registros_start + n_registros_end as n_registros_shape,
    from 
        distancia d
    left join (
            select distinct id_viagem, sentido_shape, distancia_inicio_fim
            from {{ ref("aux_viagem_circular") }}
        ) c
    on c.id_viagem = d.id_viagem
    and c.sentido_shape = d.sentido_shape
)
-- 2. Calcula distancia total por viagem - junta distancias corrigidas
--    de ida e volta de viagens circulares.
select
    id_viagem,
    sum(distancia_aferida) as distancia_aferida,
    sum(n_registros_middle) as n_registros_middle,
    sum(n_registros_start) as n_registros_start,
    sum(n_registros_end) as n_registros_end,
    sum(n_registros_out) as n_registros_out,
    sum(n_registros_total) as n_registros_total,
    sum(n_registros_minuto) as n_registros_minuto,
    sum(n_registros_shape) as n_registros_shape,
    '{{ var("version") }}' as versao_modelo
from
    inicio_fim
group by 1