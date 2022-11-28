{{
  config(
    materialized='ephemeral'
  )
}}

with realocacao as (
  select 
    * except(dataEntrada, dataSaida),
  case
    -- LIMITE: intervalo começou há + 1 hora atrás => aceitamos apenas a realocação dentro de 1 hora
    when dataEntrada < datetime_sub(dataOperacao, interval 1 hour) then datetime_sub(dataOperacao, interval 1 hour)
    else dataEntrada
  end as dataEntrada,
  case
    -- LIMITE: intervalo termina após a realocacao => não aceitamos premonições
    when dataSaida > dataOperacao or dataSaida is null then dataOperacao
    else dataSaida
  end as dataSaida
  from {{ ref('sppo_registros_realocacao') }}
  where
    -- DESCARTE: intervalo começou após a realocação
    dataEntrada <= dataOperacao
    and data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
    and dataOperacao between "{{var('date_range_start')}}" and "{{var('date_range_end')}}"
    -- DESCARTE: inicio do processo de realocações
    and data >= date("{{var('data_inicio_realocacao')}}")
),
gps as (
  select
    id_veiculo,
    timestamp_gps,
    linha
  from {{ ref('sppo_aux_registros_filtrada') }}
  where
    data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
    and timestamp_gps > "{{var('date_range_start')}}" and timestamp_gps <= "{{var('date_range_end')}}"
),
combinacao as (
  select 
    g.id_veiculo,
    g.timestamp_gps,
    r.linha,
    r.dataOperacao as datetime_realocacao
  from gps g
  inner join realocacao r
  on 
    g.id_veiculo = r.veiculo
    and g.linha != r.linha
    and g.timestamp_gps between r.dataEntrada and r.dataSaida
)
-- Filtra realocacao mais recente para cada timestamp
select 
  * except(rn)
from (
  select 
    *,
    row_number() over (partition by id_veiculo, timestamp_gps order by datetime_realocacao) as rn
  from combinacao
)
where rn = 1