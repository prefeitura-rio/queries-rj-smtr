with conformidade as (
  select 
    *,
    case 
      when perc_conformidade_shape < 80 then "Não atingiu conformidade de shape"
      when perc_conformidade_registros < 50 then "Não atingiu conformidade de GPS"
      else "Deferido"
    end as julgamento,
    concat("Serviços associados ao veículo no período da viagem:", servico_informado) as observacao
  from 
    `rj-smtr-dev`.`projeto_subsidio_sppo`.`aux_recurso_viagem_conformidade`
)
select
  r.* except(julgamento, observacao),
  ifnull(coalesce(r.julgamento, c.julgamento), "Sem sinal de GPS") as julgamento,
  c.id_viagem,
  c.perc_conformidade_shape,
  c.perc_conformidade_registros,
  case
    when c.observacao is not null then c.observacao else r.observacao
  end as observacao
from `rj-smtr-dev`.`projeto_subsidio_sppo`.`aux_recurso_avaliado` r
left join conformidade c
on r.id_recurso = c.id_recurso;