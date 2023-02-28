-- TODO: rever tratamento
WITH
  veiculo_licenciado AS (
  SELECT
    id_veiculo,
    placa,
    tipo_veiculo,
    status,
    -- Licenciado, Em andamento
    indicador_ar_condicionado AS indicador_veiculo_com_ar,
  FROM
    {{ ref("sppo_licenciamento") }}
  -- TODO: passar para data_versao
  WHERE
    data = "2023-02-16"),
  veiculo_licenciado_dia AS (
  SELECT
    data,
    l.*
  FROM
    veiculo_licenciado l
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY("2023-01-16", "2023-01-31")) data),
  veiculo_gps_dia AS (
  SELECT
    DISTINCT data,
    id_veiculo
  FROM
    `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
  WHERE
    data BETWEEN DATE("2023-01-16")
    AND DATE("2023-01-31") ),
  -- 2. Recupera dados de autuacao por operacao sem ar (Podem existir veiculos com multiplas autuacoes no dia)
  veiculo_infracao AS (
  SELECT
    DISTINCT placa,
    data_infracao,
    TRUE AS indicador_veiculo_autuado
  FROM
    `rj-smtr-dev`.`operacao`.`sppo_infracao`
  WHERE
    -- TODO: passar para data_versao
    data = "2023-02-07"
    AND data_infracao BETWEEN DATE("2023-01-16")
    AND DATE("2023-01-31")
    AND modo = "ONIBUS"
    AND id_infracao = "023.II" ),
  -- 3. Cria lista de veiculos ativos no dia (licenciados ou com sinal de GPS) e adiciona indicadores
  veiculo_indicador AS (
  SELECT
    COALESCE(l.data, g.data) AS data,
    COALESCE(l.id_veiculo, g.id_veiculo) AS id_veiculo,
    -- 3.1. Status dos veiculos ativos no dia
    IFNULL(status, "Nao licenciado") AS status,
    -- 3.2. Se operou no dia (enviou algum sinal de GPS)
    CASE
      WHEN g.id_veiculo IS NULL THEN FALSE
    ELSE
    TRUE
  END
    AS indicador_veiculo_operacao,
    -- 3.3. Se possui ar condicionado
    COALESCE(l.indicador_veiculo_com_ar, NULL) AS indicador_veiculo_com_ar,
    -- 3.4. Se foi autuado no dia
    COALESCE(i.indicador_veiculo_autuado, FALSE) AS indicador_veiculo_autuado
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
    AND l.data = i.data_infracao )
  -- 4. Gera classificacao final do veiculo
SELECT
  *,
  CASE
    WHEN status = "Licenciado" AND indicador_veiculo_com_ar AND NOT indicador_veiculo_autuado THEN 1
    WHEN status = "Licenciado"
  AND indicador_veiculo_com_ar
  AND indicador_veiculo_autuado THEN 2
    WHEN status = "Licenciado" AND NOT indicador_veiculo_com_ar THEN 3
    WHEN status = "Em andamento"
  AND indicador_veiculo_com_ar
  AND NOT indicador_veiculo_autuado THEN 4
    WHEN status = "Em andamento" AND indicador_veiculo_com_ar AND indicador_veiculo_autuado THEN 5
    WHEN status = "Em andamento"
  -- TODO: remover essa condicao
  AND NOT indicador_veiculo_com_ar THEN 6
    WHEN status = "Validacao" AND indicador_veiculo_com_ar AND NOT indicador_veiculo_autuado THEN 7
    WHEN status = "Validacao"
  AND indicador_veiculo_com_ar
  AND indicador_veiculo_autuado THEN 8
    WHEN status = "Validacao" AND NOT indicador_veiculo_com_ar THEN 9
    WHEN status = "Nao licenciado"
  AND NOT indicador_veiculo_autuado THEN 10
  -- WHEN status = "Nao licenciado" and indicador_veiculo_autuado THEN 11 -- nao existe pois nao tem placa
  ELSE
  NULL
END
  AS id_classificacao
FROM
  veiculo_indicador