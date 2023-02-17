SELECT
  vc.*,
  vd.* EXCEPT(data,
    id_veiculo)
FROM (
  SELECT
    data,
    id_viagem,
    servico_realizado AS servico,
    sentido,
    distancia_planejada as km_planejada,
    id_veiculo
  FROM
    {{ ref("viagem_completa") }}
  WHERE
    DATA BETWEEN "2023-01-16"
    AND "2023-01-31" ) vc
LEFT JOIN (
  SELECT
    data,
    id_veiculo,
    id_classificacao
  FROM
    {{ ref("sppo_veiculo_dia") }} AS vd
  WHERE
    DATA BETWEEN "2023-01-16"
    AND "2023-01-31" ) vd
ON
  vd.id_veiculo = vc.id_veiculo
  AND vd.data = vc.data