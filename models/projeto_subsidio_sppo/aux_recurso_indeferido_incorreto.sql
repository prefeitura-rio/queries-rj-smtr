with recursos as (
  SELECT 
    safe_cast(modo as string) as modo,
    cast(safe_cast(id_recurso as float64) as string) as id_recurso,
    safe_cast(split(data_recurso,".")[OFFSET(0)] as datetime) as data_recurso,
    safe_cast(protocolo as string) as protocolo,
    safe_cast(data_viagem as date) as data_viagem,
    extract(time from safe_cast(split(hora_partida,".")[OFFSET(0)] as datetime)) as hora_partida,
    extract(time from safe_cast(split(hora_chegada,".")[OFFSET(0)] as datetime)) as hora_chegada,
    safe_cast(id_veiculo as string) as id_veiculo,
    safe_cast(servico as string) as servico,
    REGEXP_EXTRACT(servico, r'[0-9]+') as linha,
    concat("S", left(safe_cast(tipo_servico as string), 1)) as tipo_servico,
    case
      when servico like "%-%" then REGEXP_EXTRACT(split(servico, "-")[OFFSET(0)], r'[A-Z]+')
      else REGEXP_EXTRACT(servico, r'[A-Z]+')
    end as tipo_servico_extra,
    left(safe_cast(sentido as string),1) as sentido,
    safe_cast(status as string) as status,
    safe_cast(timestamp_captura as timestamp) as timestamp_captura,
    data,
    hora
  FROM {{ var("recurso_staging") }} -- `rj-smtr-dev.projeto_subsidio_sppo_staging.recursos_filtrada`
),
-- Filtra viagens no periodo avaliado
recursos_filtrada as (
  select r.*, p.data_prazo_recurso
  from recursos r
  inner join {{ var("recurso_prazo" )}} p -- `rj-smtr-dev.projeto_subsidio_sppo.recurso_prazo` p
  on r.data_viagem between p.data_inicio_viagem and p.data_fim_viagem
),
-- Avalia informacoes incorretas quanto a servico, id_veiculo e data da viagem
recursos_tratada as (
  select 
    protocolo,
    datetime(data_viagem, hora_partida) as datetime_partida,
    datetime(data_viagem, hora_chegada) as datetime_chegada,
    case 
      -- caso 1: linha regular => SA
      when tipo_servico_extra is null and tipo_servico = "SA" then linha
      -- caso 2: lecd
      when tipo_servico_extra = "LECD" and tipo_servico = "SA" then concat(tipo_servico_extra, linha)
      -- caso 3: linha com servico nao regular => SV/SVB
      when tipo_servico_extra is null and tipo_servico != "SA" then concat(tipo_servico, linha)
      when tipo_servico_extra is not null and STARTS_WITH(tipo_servico_extra, tipo_servico) then concat(tipo_servico_extra, linha)
      else concat("Não identificado: ", servico, " / ", tipo_servico)
    end as servico
    from recursos_filtrada
),
-- -- Avalia recursos para indeferimento
recursos_indeferidos as (
  select
    data,
    hora,
    timestamp_captura,
    modo, 
    id_recurso,
    r.protocolo,
    status,
    data_prazo_recurso,
    data_recurso,
    data_viagem,
    t.datetime_partida,
    t.datetime_chegada,
    t.servico,
    sentido,
    id_veiculo,
    case 
      when datetime_partida > datetime_chegada then "Fim da viagem incorreto."
      when datetime_partida > data_recurso then "Início da viagem incorreto."
      when t.servico like "Não identificado:%" then "Linha e tipo de serviço não correspondem."
      when sentido not in ("I", "V", "C") then "Sentido incorreto."
      when length(id_veiculo) != 5 and length(REGEXP_EXTRACT(id_veiculo, r'[0-9]+')) != 5 then "Número de ordem incorreto - não possui 5 dígitos."
      when extract(date from data_recurso) > date(data_prazo_recurso) then "Recurso fora do prazo."
    end as observacao
  from recursos_filtrada r
  inner join recursos_tratada t
  on r.protocolo = t.protocolo
)
select
  *,
  case
    when observacao is not null then "Indeferido"
  end as julgamento,
  case
    when observacao = "Recurso fora do prazo." then "Intempestivo. Recurso de viagem fora do prazo.  (Art. 5o Res. SMTR 3534 / 2022)"
    when observacao is not null then "Informação incompleta ou incorreta (Art. 5o § 2o Res. SMTR 3534 / 2022)"
  end as motivo,
  null as distancia_paga
from recursos_indeferidos