{{ 
config(
    materialized='incremental',
    partition_by={
            "field":"data_viagem",
            "data_type": "date",
            "granularity":"day"
    }
)
}}
with viagem_conformidade as (
  select 
    protocolo,
    concat(
      "ID da viagem identificada: ",
      id_viagem,
      "Serviços associados ao veículo no período da viagem: ", 
      servico_informado,
      "Itinerário (shape) do serviço informado no recurso: ",
      trip_id,
      "\nPercentual de conformidade do itinerário: ",
      perc_conformidade_shape,
      "\nPercentual de conformidade do GPS: ",
      perc_conformidade_registros,
      "\nPercentual de conformidade da distancia: ",
      perc_conformidade_distancia
    ) as observacao,
    case
      when perc_conformidade_shape < {{ var("perc_conformidade_shape_min") }} 
        or perc_conformidade_registros < {{ var("perc_conformidade_registros_min") }}
        or perc_conformidade_distancia <= {{ var("perc_conformidade_distancia_recurso_min") }}
      then "Indeferido"
      else "Deferido"
    end as julgamento,
    case 
      when perc_conformidade_shape < {{ var("perc_conformidade_shape_min") }} 
      then "Não respeitou o limite da conformidade da quantidade de transmissões dentro do itinerário. (Art 2o Res. SMTR 3534 / 2022)"
      when perc_conformidade_registros < {{ var("perc_conformidade_registros_min") }} 
      then "Não respeitou o limite da conformidade da qualidade do GPS. (Art 2o Res. SMTR 3534 / 2022)"
      when perc_conformidade_distancia <= {{ var("perc_conformidade_distancia_recurso_min") }}
      then "Não respeitou o limite da conformidade da distância da viagem."
      else "Viagem identificada considerando os sinais de GPS com o serviço informado pelo recurso."
    end as motivo,
  from 
    {{ ref("aux_recurso_viagem_conformidade") }}
)
select
  r.* except(observacao,  julgamento, motivo),
  ifnull(
    coalesce(r.observacao, cf.observacao), 
    "Indeferido por ausência de sinal de GPS ou serviço não planejado na data da viagem para apuração."
  ) as observacao,
  ifnull(
    coalesce(r.julgamento, cf.julgamento),
    "Indeferido"
  ) as julgamento,
  ifnull(
    coalesce(r.motivo, cf.motivo), 
    "Não houve comunicação de GPS no período informado para o veículo. (Art 1o Res. SMTR 3534 / 2022)"
  ) as motivo,
from {{ ref("aux_recurso_indeferido_viagem_paga") }} r
left join viagem_conformidade cf
on r.protocolo = cf.protocolo