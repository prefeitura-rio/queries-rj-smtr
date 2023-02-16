WITH
  viagem_completa AS (
  SELECT
    *
  FROM
    {{ ref("viagem_completa") }}
  WHERE
    DATA BETWEEN DATE("2023-01-16")
    AND DATE("2023-01-31") )
SELECT
  vc.*,
  CASE
    WHEN vd.indicador_veiculo_licenciado = FALSE THEN 1
    WHEN vd.indicador_veiculo_com_ar = FALSE THEN 2
    WHEN (vd.indicador_veiculo_com_ar = TRUE AND vd.indicador_veiculo_autuado = TRUE) THEN 3
    WHEN (vd.indicador_veiculo_com_ar = TRUE AND vd.indicador_veiculo_autuado = FALSE) THEN 4
    ELSE 999
  END AS id_classificacao
FROM
  viagem_completa AS vc
LEFT JOIN
  {{ ref("veiculo_dia") }} AS vd
ON
  vc.id_veiculo = vd.id_veiculo
  AND vc.data = vd.data