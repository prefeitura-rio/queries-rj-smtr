{{ 
config(
    materialized='ephemeral'
)
}}

-- 1. Corrije preenchimento de dados
with recursos_tratada as (
  select
    timestamp_captura,
    data_recurso,
    protocolo,
    data_viagem,
    datetime(data_viagem, hora_partida) as datetime_partida,
    case
      when hora_partida > hora_chegada and hora_chegada <= time(03,00,00)
      then datetime(date_add(data_viagem, interval 1 day), hora_chegada)
      else datetime(data_viagem, hora_chegada)
    end as datetime_chegada,
    case 
      -- caso 1: linha regular => SA
      when tipo_servico_extra is null and tipo_servico = "SA" then linha
      -- caso 2: lecd
      when tipo_servico_extra = "LECD" and tipo_servico = "SA" then concat(tipo_servico_extra, linha)
      -- caso 3: linha com servico nao regular => SV/SVB
      when tipo_servico_extra is null and tipo_servico != "SA" then concat(tipo_servico, linha)
      when tipo_servico_extra is not null and STARTS_WITH(tipo_servico_extra, tipo_servico) then concat(tipo_servico_extra, linha)
      else concat("Não identificado: ", servico, " / ", tipo_servico)
    end as servico,
    sentido,
    id_veiculo
  from {{ ref('recurso_filtrada') }}
),
-- 2. Avalia informações incorretas
recursos_incorretos as (
  select
    *,
    case 
      when datetime_partida > datetime_chegada then "Fim da viagem incorreto."
      when datetime_partida > data_recurso then "Início da viagem incorreto."
      when servico like "Não identificado:%" then "Linha e tipo de serviço não correspondem."
      when sentido not in ("I", "V", "C") then "Sentido incorreto."
      when length(id_veiculo) != 5 and length(REGEXP_EXTRACT(id_veiculo, r'[0-9]+')) != 5 then "Número de ordem incorreto - não possui 5 dígitos."
    end as observacao
  from recursos_tratada
)
select
  *,
  case 
    when observacao is not null
    then 1
    else null 
  end as id_julgamento
from recursos_incorretos