{{ 
config(
    materialized='incremental',
    partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
    },
    unique_key="data",
    incremental_strategy = 'insert_overwrite'
)
}}

-- ==> aux_registros_status_trajeto = aux_registros_status_viagem
with recursos as (
  select *
  FROM {{ ref("aux_recurso_viagem_paga") }}
  where id_julgamento is null
  {# {% if is_incremental() -%} #}
    and data_viagem between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
  {# {% endif -%} #}
),
gps_viagem as (
  SELECT 
    r.protocolo,
    r.data_viagem as data,
    g.id_veiculo,
    substr(g.id_veiculo, 2, 3) as id_empresa,
    r.datetime_partida, # Partida informada no recurso
    r.datetime_chegada, # Chegada informada no recurso
    r.sentido, -- Sentido informado no recurso
    datetime_diff(r.datetime_chegada, r.datetime_partida, minute) + 1 as tempo_viagem, # Tempo da viagem com base no recurso
    g.timestamp_gps,
    timestamp_trunc(timestamp_gps, minute) as timestamp_minuto_gps,
    ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
    TRIM(r.servico, " ") as servico_realizado, -- Servico informado no recurso
    TRIM(g.servico, " ") as servico_informado, -- Servico informado no GPS
    g.distancia,
  from recursos r
  left join  (
    select 
      id_veiculo,
      longitude, 
      latitude,
      servico,
      timestamp_gps,
      timestamp_trunc(timestamp_gps, minute) as timestamp_minuto_gps,
      distancia
    from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`  -- TODO: ref in prod
    where status != "Parado garagem"
    {# {% if is_incremental() -%} #}
      and data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
      and timestamp_gps between '{{ var("recurso_viagem_start")}}' and datetime_add('{{ var("recurso_viagem_end")}}', interval 3 hour)
    {# {% endif -%} #}
   ) g
  on
    r.id_veiculo = substr(g.id_veiculo, 2)
    and cast(g.timestamp_gps as datetime) between datetime_partida and datetime_chegada
  where g.servico is not null
),
registros_status_viagem as (
  select
    concat(g.id_veiculo, "-", g.servico_realizado,"-", g.sentido, "-", FORMAT_DATETIME("%Y%m%d%H%M%S", g.datetime_partida)) as id_viagem,
    g.* except(distancia),
    s.shape_id,
    s.sentido_shape,
    s.shape_id_planejado,
    s.trip_id,
    s.trip_id_planejado,
    s.start_pt,
    s.end_pt,
    s.distancia_planejada,
    ifnull(g.distancia,0) as distancia,
    case
        when ST_DWITHIN(g.posicao_veiculo_geo, start_pt, 500)
        then 'start'
        when ST_DWITHIN(g.posicao_veiculo_geo, end_pt, 500)
        then 'end'
        when ST_DWITHIN(g.posicao_veiculo_geo, shape, 500)
        then 'middle'
    else 'out'
    end status_viagem,
    '{{ var("version") }}' as versao_modelo
  from 
    gps_viagem g
  inner join (
    select 
      *
    from (
      select 
        * except(trip_id, shape_id, shape, distancia_planejada),
        trip_id, shape_id, shape, distancia_planejada
      from
          `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
      where sentido != "C"
      {# {% if is_incremental() -%} #}
        and data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
      {# {% endif -%} #}
    )
    union all (
      select 
        * except(shape, shape_volta, distancia_planejada, distancia_planejada_volta),
        st_geogfromtext(
          concat(
            "LINESTRING(", 
            replace(TRIM(ST_ASTEXT(ST_UNION(shape, shape_volta)), "MULTILINESTRING()"), "), (", ","), 
            ")"
          )
        ) as shape,
        distancia_planejada + distancia_planejada_volta as distancia_planejada
      from (
        select 
          * except(trip_id, shape_id),
          concat(SUBSTR(trip_id, 1, 10), sentido, SUBSTR(trip_id, 12, length(trip_id))) as trip_id,
          concat(SUBSTR(shape_id, 1, 10), sentido, SUBSTR(shape_id, 12, length(shape_id))) as shape_id,
          lead(shape) over (partition by data, servico order by sentido_shape) as shape_volta,
          lead(distancia_planejada) over (partition by data, servico order by sentido_shape) as distancia_planejada_volta
        from
            `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
        where sentido = "C"
        {# {% if is_incremental() -%} #}
          and data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
        {# {% endif -%} #}
        ) 
        where sentido_shape = "I"
    )
  ) s
  on
    g.data = s.data
    and g.servico_realizado = s.servico
    and g.sentido = s.sentido
),
-- ==> aux_viagem_registros
-- 1. Calcula a distância total percorrida por viagem, separada por
--    shape. Adiciona distância do 1o/último sinal de gps ao início/final do
--    shape. Isso é necessário pois o 1o/ultimo sinal é contabilizado
--    apenas quando o veiculo sai/chega dentro do raio de 500m ao redor
--    do ponto inicial/final. Contabiliza também o número de registros
--    em cada tapa da viagem (inicio, meio, fim, fora), total de
--    registros de gps e total de minutos da viagem com registros de gps.
distancia as (
    select 
        *,
        n_registros_middle + n_registros_start + n_registros_end as n_registros_shape
    from (
        select distinct
            id_viagem, -- C // I
            trip_id,  -- I/V // I/V
            sentido,
            sentido_shape,
            distancia_planejada,
            0 as distancia_inicio_fim,
            round(sum(distancia)/1000, 3) as distancia_aferida,
            sum(case when status_viagem = "middle" then 1 else 0 end) as n_registros_middle,
            sum(case when status_viagem = "start" then 1 else 0 end) as n_registros_start,
            sum(case when status_viagem = "end" then 1 else 0 end) as n_registros_end,
            sum(case when status_viagem = "out" then 1 else 0 end) as n_registros_out,
            count(timestamp_gps) as n_registros_total,
            count(distinct timestamp_minuto_gps) as n_registros_minuto
        from (
            select distinct * except(posicao_veiculo_geo, start_pt, end_pt)
            from registros_status_viagem
            -- where
            --     data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
        )
        group by 1,2,3,4,5
    )
),
-- 2. Calcula distancia total por viagem - junta distancias corrigidas
--    de ida e volta de viagens circulares. 
aux_viagem_registros as (
  select *
  from (
    select
      id_viagem,
      sum(distancia_planejada) as distancia_planejada,
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
    where sentido = "C"
    group by 1
  )
  union all (
    select
      id_viagem,
      distancia_planejada,
      distancia_aferida,
      distancia_inicio_fim,
      n_registros_middle,
      n_registros_start,
      n_registros_end,
      n_registros_out,
      n_registros_total,
      n_registros_minuto,
      n_registros_shape,
      '{{ var("version") }}' as versao_modelo
    from
        distancia
    where sentido = sentido_shape
  )
),
-- (Adicional) Junta diferentes serviços informados por GPS ao longo viagem numa única string
servicos_gps_viagem as (
  select 
    protocolo,
    id_viagem,
    STRING_AGG(distinct servico_informado, ', ') as servico_informado
  from registros_status_viagem
  group by 1,2
)
-- => aux_viagem_conformidade:
-- 2. Calcula os percentuais de conformidade da distancia, trajeto e GPS
select distinct
  s.protocolo,
  v.id_viagem, 
  v.data,
  v.id_empresa,
  v.id_veiculo,
  s.servico_informado,
  v.servico_realizado,
  d.distancia_planejada,
  v.sentido,
  v.datetime_partida,
  v.datetime_chegada,
  v.trip_id,
  v.shape_id,
  v.tempo_viagem,
  d.* except(id_viagem, distancia_planejada, versao_modelo),
  round(100 * n_registros_shape/n_registros_total, 2) as perc_conformidade_shape,
  round(100 * d.distancia_aferida/d.distancia_planejada, 2) as perc_conformidade_distancia,
  round(100 * n_registros_minuto/tempo_viagem, 2) as perc_conformidade_registros,
  '{{ var("version") }}' as versao_modelo
from 
    registros_status_viagem v
inner join 
    aux_viagem_registros d
on
    v.id_viagem = d.id_viagem
inner join servicos_gps_viagem s
on
  v.id_viagem = s.id_viagem