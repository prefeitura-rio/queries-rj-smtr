{{ config(
  materialized = "table",
) }}

WITH
  recurso AS (
  SELECT
    DISTINCT DATA,
    servico
  FROM
    (
      SELECT
        data,
        servico
      FROM
        {{ ref("encontro_contas_recursos_sppo_servico_dia_pago") }}
      UNION ALL
      SELECT
        data,
        servico
      FROM
        {{ ref("recursos_sppo_servico_dia_avaliacao") }}
    )
  ),
  subsidio AS (
  SELECT
    s.*
  FROM (
    SELECT
      EXTRACT(year
      FROM
        DATA) AS ano,
      DATA,
      consorcio,
      servico,
      km_apurada AS km_apurada,
      valor_subsidio_pago AS subsidio
    FROM
      {{ ref("sumario_servico_dia_historico") }}
    WHERE
      DATA BETWEEN "2023-01-01"
      AND "2023-11-30"
      AND DATA NOT IN ("2022-10-02",
        "2022-10-30",
        "2023-02-17",
        "2023-02-18",
        "2023-02-19",
        "2023-02-20",
        "2023-02-21",
        "2023-02-22") ) s
  LEFT JOIN
    recurso r
  USING
    (DATA,
      servico)
  WHERE
    r.data IS NULL),
  desconto AS (
  SELECT
    EXTRACT(year
    FROM
      DATA) AS ano,
    s.data,
    s.consorcio,
    s.servico,
    SUM(s.km_apurada * desconto_subsidio_km) AS desconto
  FROM (
    SELECT
      DATA,
      consorcio,
      servico,
      tipo_viagem,
      km_apurada
    FROM
      {{ ref("sumario_servico_tipo_viagem_dia") }}
    WHERE
      DATA BETWEEN "2023-01-01"
      AND "2023-11-30") s
  LEFT JOIN (
    SELECT
      DISTINCT data_inicio,
      data_fim,
      CASE
        WHEN status = "Licenciado com ar e autuado (023.II)" THEN "Autuado por ar inoperante"
        WHEN status = "Licenciado sem ar" THEN "Licenciado sem ar e não autuado"
        WHEN status = "Licenciado com ar e não autuado (023.II)" THEN "Licenciado com ar e não autuado"
      ELSE
      status
    END
      AS tipo_viagem,
      irk,
      irk_tarifa_publica,
      desconto_subsidio_km
    FROM
      {{ source("projeto_subsidio_sppo_encontro_contas", "subsidio_parametros_atualizada") }} ) p
  ON
    (s.data BETWEEN p.data_inicio
      AND p.data_fim)
    AND s.tipo_viagem = p.tipo_viagem
  GROUP BY
    1,
    2,
    3,
    4 ),
  esperado AS (
  SELECT
    s.ano,
    s.consorcio,
    SUM(km_apurada) AS km,
    SUM(subsidio) AS subsidio,
    SUM(9.17 * km_apurada) AS receita_esperada,
    SUM(desconto) AS desconto
  FROM
    subsidio s
  LEFT JOIN
    recurso r
  USING
    (DATA,
      servico)
  LEFT JOIN
    desconto d
  USING
    (DATA,
      servico)
  GROUP BY
    1,
    2 ),
  rdo AS (
  SELECT
    rdo.ano,
    c.consorcio,
    SUM(rdo.remuneracao_tarifaria) AS remuneracao_tarifaria
  FROM (
    SELECT
      EXTRACT(year
      FROM
        DATA) AS ano,
      DATA,
      termo AS id_consorcio,
      CASE
        WHEN LENGTH(linha) < 3 THEN LPAD(linha, 3, "0")
      ELSE
      CONCAT( IFNULL(REGEXP_EXTRACT(linha, r"[B-Z]+"), ""), IFNULL(REGEXP_EXTRACT(linha, r"[0-9]+"), "") )
    END
      AS servico,
      SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie) AS remuneracao_tarifaria
    FROM
      {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }}
    WHERE
      DATA BETWEEN DATE("2023-01-01")
      AND DATE("2023-11-30")
      AND DATA NOT IN ("2022-10-02",
        "2022-10-30",
        "2023-02-17",
        "2023-02-18",
        "2023-02-19",
        "2023-02-20",
        "2023-02-21",
        "2023-02-22")
    GROUP BY
      1,
      2,
      3,
      4 ) rdo
  INNER JOIN (
    SELECT
      consorcio,
      id_consorcio
    FROM
      rj-smtr.cadastro.consorcios
    WHERE
      consorcio IN ("Internorte",
        "Intersul",
        "Santa Cruz",
        "Transcarioca") ) c
  USING
    (id_consorcio)
  LEFT JOIN
    recurso r
  USING
    (DATA,
      servico)
  WHERE
    r.data IS NULL
  GROUP BY
    1,
    2 )
SELECT
  ano,
  consorcio,
  ROUND(km/POW(10,6), 2) km_milhoes,
  ROUND(remuneracao_tarifaria/POW(10,6), 2) remuneracao_tarifaria_milhoes,
  ROUND(subsidio/POW(10,6), 2) subsidio_milhoes,
  ROUND(receita_aferida/POW(10,6), 2) AS receita_aferida_milhoes,
  ROUND(receita_esperada/POW(10,6), 2) AS receita_esperada_milhoes,
  ROUND(desconto/POW(10,6), 2) AS desconto_milhoes,
  ROUND((receita_aferida - receita_esperada + desconto)/POW(10, 6), 2) AS saldo
FROM (
  SELECT
    e.*,
    rdo.remuneracao_tarifaria,
    rdo.remuneracao_tarifaria + e.subsidio AS receita_aferida,
  FROM
    esperado e
  FULL JOIN
    rdo
  USING
    (ano,
      consorcio) )
ORDER BY
  1,
  2