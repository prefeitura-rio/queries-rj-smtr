SELECT
  id_classificacao,
  CASE
    WHEN id_classificacao = 1 THEN "Veículo não licenciado"
    WHEN id_classificacao = 2 THEN "Veículo sem ar condicionado"
    WHEN id_classificacao = 3 THEN "Veículo com ar condicionado e autuado por inoperância"
    WHEN id_classificacao = 4 THEN "Veículo sem irregularidade identificada"
  ELSE
  "Motivo não identificado"
END
  AS descricao
FROM
  UNNEST(GENERATE_ARRAY(1, 4, 1)) AS id_classificacao