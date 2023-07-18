-- 1. Calcula a distância total percorrida por viagem, separada por
--    shape. Adiciona distância do 1o/último sinal de gps ao início/final do
--    shape. Isso é necessário pois o 1o/ultimo sinal é contabilizado
--    apenas quando o veiculo sai/chega dentro do raio de 500m ao redor
--    do ponto inicial/final. Contabiliza também o número de registros
--    em cada tapa da viagem (inicio, meio, fim, fora), total de
--    registros de gps e total de minutos da viagem com registros de gps.
with distancia as (
    select 
        *,
        n_registros_middle + n_registros_start + n_registros_end as n_registros_shape
    from (
        select distinct
            id_viagem,
            trip_id,
            max(distancia_inicio_fim) as distancia_inicio_fim,
            round(sum(distancia)/1000 + max(distancia_inicio_fim), 3) as distancia_aferida,
            sum(case when status_viagem = "middle" then 1 else 0 end) as n_registros_middle,
            sum(case when status_viagem = "start" then 1 else 0 end) as n_registros_start,
            sum(case when status_viagem = "end" then 1 else 0 end) as n_registros_end,
            sum(case when status_viagem = "out" then 1 else 0 end) as n_registros_out,
            count(timestamp_gps) as n_registros_total,
            count(distinct timestamp_minuto_gps) as n_registros_minuto
        from (
            select distinct * except(posicao_veiculo_geo, start_pt, end_pt)
            from {{ ref("registros_status_viagem") }}
            where
                data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
        )
        group by 1,2
    )
)
-- 2. Calcula distancia total por viagem - junta distancias corrigidas
--    de ida e volta de viagens circulares. 
select
    id_viagem,
    sum(distancia_aferida) as distancia_aferida,
    sum(distancia_inicio_fim) as distancia_inicio_fim,
    sum(n_registros_middle) as n_registros_middle,
    sum(n_registros_start) as n_registros_start,
    sum(n_registros_end) as n_registros_end,
    sum(n_registros_out) as n_registros_out,
    sum(n_registros_total) as n_registros_total,
    sum(n_registros_minuto) as n_registros_minuto,
    sum(n_registros_shape) as n_registros_shape,
    '{{ var("version") }}' as versao_modelo
from
    distancia
group by 1