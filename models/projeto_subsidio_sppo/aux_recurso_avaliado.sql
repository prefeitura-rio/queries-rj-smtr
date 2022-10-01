with recursos as (
  -- Avalia recursos com julgamento do tipo "Fora do prazo" e "Viagem paga".
  select
    r.*,
    case
      -- when r.data_recurso not between r.datetime_partida and date_add(r.datetime_partida, interval 30 DAY) then "Recurso fora do prazo" -- TODO: vamos considerar por enquanto
      when v.id_viagem is not null then "Viagem já paga."
    end as julgamento,
    case 
      when r.data_recurso not between r.datetime_partida and date_add(r.datetime_partida, interval 30 DAY) then "Recurso fora do prazo"
      when v.id_viagem is not null then concat("Existe viagem paga no horário: ", id_viagem) -- TODO: remover duplicacao ida, volta para circular?
    end as observacao
  FROM rj-smtr-dev.projeto_subsidio_sppo.aux_recurso_filtrada r
  left join (
    select * from rj-smtr-dev.projeto_subsidio_sppo.viagem_completa
    where data between "2022-07-01" and "2022-07-31"
  ) v
  on r.id_veiculo = substr(v.id_veiculo, 2,6) 
  and r.datetime_partida <= v.datetime_partida and r.datetime_chegada >= v.datetime_chegada
),
sobreposto as (
  -- Filtra e avalia recursos com sobreposição
  select 
    id_recurso,
    julgamento,
    concat("Existe sopreposição da viagem com o seguinte recurso: ", string_agg(sobreposicao, ", ")) as observacao
  from (
    SELECT
      s1.id_recurso,
      "Pedido de recurso sobreposto." as julgamento,
      s2.id_recurso as sobreposicao,
      row_number() over (partition by s1.id_recurso) as rn
    FROM (
      select * from recursos where julgamento is null
    ) s1
    inner join (
      select * from recursos where julgamento is null
    ) s2
    on 
      s1.id_veiculo = s2.id_veiculo
      and s1.datetime_partida <= s2.datetime_partida
      and s2.datetime_partida < s1.datetime_chegada
    where s1.id_recurso != s2.id_recurso
    order by 1,2
  )
  group by 1,2
)
select
  r.* except(julgamento, observacao),
  coalesce(s.julgamento, r.julgamento) as julgamento,
  coalesce(s.observacao, r.observacao) as observacao
from recursos r
left join sobreposto s
on r.id_recurso = s.id_recurso;