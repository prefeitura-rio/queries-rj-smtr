{{ 
config(
    partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    },
)
}}

WITH
  -- 1. Recupera serviços que operaram
  servico_dia AS (
  SELECT
    DISTINCT `data`,
    consorcio,
    servico,
    viagens,
    km_apurada AS quilometragem,
    valor_subsidio_pago AS subsidio
  FROM
    {{ ref("sumario_servico_dia_historico") }}
  WHERE
    `data` BETWEEN "2022-06-01"
    AND "2023-05-31"
    AND `data` NOT IN ("2022-10-02",
      "2022-10-30",
      "2023-02-17",
      "2023-02-18",
      "2023-02-19",
      "2023-02-20",
      "2023-02-21",
      "2023-02-22") --
    AND valor_subsidio_pago > 0 ),
  -- 2. Remove serviços-dia pagos por recurso
  recurso AS (
  SELECT
    DISTINCT data_viagem AS `data`,
    servico,
  FROM
    `rj-smtr.projeto_subsidio_sppo_encontro_contas.recursos_sppo_reprocessamento`
  WHERE
    data_viagem BETWEEN "2022-06-01"
    AND "2023-05-31"
  UNION ALL (
    SELECT
      DISTINCT data_viagem AS `data`,
      servico
    FROM
      `rj-smtr.projeto_subsidio_sppo_encontro_contas.recursos_sppo_bloqueio_via`
    WHERE
      data_viagem BETWEEN "2022-06-01"
      AND "2023-05-31"
      AND data_viagem NOT IN ("2022-10-02",
        "2022-10-30",
        "2023-02-17",
        "2023-02-18",
        "2023-02-19",
        "2023-02-20",
        "2023-02-21",
        "2023-02-22") ) ),
  servico_dia_valido AS (
  SELECT
    s.*
  FROM
    servico_dia s
  LEFT JOIN
    recurso r
  USING
    (`data`,
      servico)
  WHERE
    r.data IS NULL ),
  -- 3. Calcula receita tariária aferida de cada serviço por dia (RDO)
  remun_tarifa AS (
  SELECT
    DISTINCT `data`,
    CASE
      WHEN LENGTH(linha) < 3 THEN LPAD(linha, 3, "0")
    ELSE
    CONCAT( IFNULL(REGEXP_EXTRACT(linha, r"[B-Z]+"), ""), IFNULL(REGEXP_EXTRACT(linha, r"[0-9]+"), "") )
  END
    AS servico,
    SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie) AS remuneracao_tarifaria
  FROM
    `rj-smtr.br_rj_riodejaneiro_rdo.rdo40_tratado`
  WHERE
    `data` BETWEEN "2022-06-01"
    AND "2023-05-31"
    AND `data` NOT IN ("2022-10-02",
      "2022-10-30",
      "2023-02-17",
      "2023-02-18",
      "2023-02-19",
      "2023-02-20",
      "2023-02-21",
      "2023-02-22")
  GROUP BY
    1,
    2 ),
  -- 4. Calcula receita tariária esperada de cada serviço por dia
  valor_km AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    irk,
    irk_tarifa_publica
  FROM
    `rj-smtr.projeto_subsidio_sppo_encontro_contas.subsidio_parametros_atualizada`
  WHERE
    data_fim >= DATE("2022-06-01")
    AND data_inicio <= DATE("2023-05-31") ),
  -- 5. Calcula saldo da receita tarifaria
  remun_servico AS (
  SELECT
    EXTRACT(year
    FROM
      `data`) AS ano,
    s.data,
    s.consorcio,
    s.servico,
    MAX(v.irk) AS irk,
    MAX(v.irk_tarifa_publica) AS irk_tarifa_publica,
    ROUND(SUM(s.viagens), 0) AS viagens,
    ROUND(SUM(s.quilometragem), 2) AS quilometragem,
    ROUND(MAX(v.irk_tarifa_publica) * SUM(IFNULL(s.quilometragem, 0)), 2) AS remuneracao_tarifaria_esperada,
    ROUND(SUM(IFNULL(r.remuneracao_tarifaria, 0)), 2) AS remuneracao_tarifaria_aferida,
    ROUND(SUM(IFNULL(s.subsidio, 0)), 2) AS subsidio
  FROM
    servico_dia_valido s
  LEFT JOIN
    remun_tarifa r
  USING
    (`data`,
      servico)
  INNER JOIN
    valor_km AS v
  ON
    s.data BETWEEN v.data_inicio
    AND v.data_fim
  GROUP BY
    1,
    2,
    3,
    4 )
SELECT
  *,
  ROUND(remuneracao_tarifaria_aferida - remuneracao_tarifaria_esperada, 2) AS saldo
FROM
  remun_servico