SELECT 
  case 
    when length(servico) < 3 then LPAD(servico, 3, "0")
    else replace(servico, " ", "")
  end as servico,
  * except(servico),
  case
    when tipo_dia = "Domingo" then "DD"
    when tipo_dia = "Sabado" then "SS"
    when tipo_dia = "Dia Útil" then "DU"
  end as variacao_itinerario
FROM {{ var("aux_viagem_planejada") }}