WITH
  veiculo_licenciado AS ( -- 5.958 veiculos
  SELECT
    id_veiculo,
    placa,
    tipo_veiculo,
    indicador_ar_condicionado as indicador_veiculo_com_ar
  FROM
    {{ ref("sppo_licenciamento") }}
  WHERE
    DATA = "2023-02-08"
    AND timestamp_captura = "2023-02-08 19:39:00-03:00"),
  veiculo_licenciado_dia AS ( --89.370 veiculo-dia
  SELECT
    DATA,
    l.*
  FROM
    veiculo_licenciado l
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY("2023-01-16", "2023-01-31")) DATA),
  veiculo_gps_dia AS ( -- 59.568 veiculo-dia
  SELECT
    DISTINCT DATA,
    id_veiculo
  FROM
    `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
  WHERE
    DATA BETWEEN DATE("2023-01-16")
    AND DATE("2023-01-31") ),
  veiculo_infracao AS ( -- 672 multas
  SELECT
    -- existem veiculos com multiplas autuacoes no dia
    DISTINCT placa,
    data_infracao,
    TRUE AS indicador_veiculo_autuado
  FROM
    {{ ref("sppo_infracao") }}
  WHERE
    DATA = "2023-02-07"
    AND data_infracao BETWEEN DATE("2023-01-16")
    AND DATE("2023-01-31")
    AND modo = "ONIBUS"
    AND id_infracao = "023.II" )
SELECT
  COALESCE(l.data, g.data) AS data,
  COALESCE(l.id_veiculo, g.id_veiculo) AS id_veiculo,
  CASE
    WHEN g.id_veiculo IS NULL THEN FALSE
  ELSE
  TRUE
END
  AS indicador_veiculo_operacao,
  CASE
    WHEN l.id_veiculo IS NULL THEN FALSE
  ELSE
  TRUE
END
  AS indicador_veiculo_licenciado,
  COALESCE(l.indicador_veiculo_com_ar, FALSE) AS indicador_veiculo_com_ar,
  --l.indicador_veiculo_com_ar AS indicador_veiculo_com_ar,
  CASE
    WHEN i.indicador_veiculo_autuado IS NULL THEN FALSE
  ELSE
  TRUE
END
  AS indicador_veiculo_autuado
FROM
  veiculo_licenciado_dia l
FULL JOIN
  veiculo_gps_dia g
ON
  l.data = g.data
  AND l.id_veiculo = g.id_veiculo
LEFT JOIN
  veiculo_infracao i
ON
  l.placa = i.placa
  AND l.data = i.data_infracao