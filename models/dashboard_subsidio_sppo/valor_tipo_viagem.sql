{{
    config(
        materialized="incremental",
        partition_by={"field": "data_inicio", "data_type": "date", "granularity": "day"},
        unique_key=["tipo_viagem", "data_inicio"],
        incremental_strategy="insert_overwrite",
    )
}}

WITH
  valor_tipo_viagem AS (
  -- Esta primeira consulta é necessária para que o Big Query compreenda que o datatype dos indicadores é BOOL
  SELECT
    STRUCT ( FALSE AS indicador_licenciado,
      FALSE AS indicador_ar_condicionado,
      FALSE AS indicador_autuacao_ar_condicionado,
      FALSE AS indicador_autuacao_seguranca,
      FALSE AS indicador_autuacao_limpeza,
      FALSE AS indicador_autuacao_equipamento,
      FALSE AS indicador_sensor_temperatura,
      FALSE AS indicador_validador_sbd ) AS indicadores,
    "Teste" AS status,
    0 AS valor_km,
    DATE("2023-01-16") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 51889/2022" AS legislacao,
    0 AS ordem
  UNION ALL
  SELECT
    STRUCT ( NULL AS indicador_licenciado,
      NULL AS indicador_ar_condicionado,
      NULL AS indicador_autuacao_ar_condicionado,
      NULL AS indicador_autuacao_seguranca,
      NULL AS indicador_autuacao_limpeza,
      NULL AS indicador_autuacao_equipamento,
      NULL AS indicador_sensor_temperatura,
      NULL AS indicador_validador_sbd ) AS indicadores,
    "Não licenciado" AS status,
    0 AS valor_km,
    DATE("2023-01-16") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 51889/2022" AS legislacao,
    1 AS ordem
  UNION ALL
  SELECT
    STRUCT ( TRUE AS indicador_licenciado,
      TRUE AS indicador_ar_condicionado,
      TRUE AS indicador_autuacao_ar_condicionado,
      NULL AS indicador_autuacao_seguranca,
      NULL AS indicador_autuacao_limpeza,
      NULL AS indicador_autuacao_equipamento,
      NULL AS indicador_sensor_temperatura,
      NULL AS indicador_validador_sbd ) AS indicadores,
    "Autuado por ar inoperante" AS status,
    0 AS valor_km,
    DATE("2023-01-16") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 51940/2023" AS legislacao,
    2 AS ordem
  UNION ALL
  SELECT
    STRUCT ( TRUE AS indicador_licenciado,
      NULL AS indicador_ar_condicionado,
      NULL AS indicador_autuacao_ar_condicionado,
      TRUE AS indicador_autuacao_seguranca,
      NULL AS indicador_autuacao_limpeza,
      NULL AS indicador_autuacao_equipamento,
      NULL AS indicador_sensor_temperatura,
      NULL AS indicador_validador_sbd ) AS indicadores,
    "Autuado por segurança" AS status,
    0 AS valor_km,
    DATE("2023-07-04") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 52820/2023" AS legislacao,
    3 AS ordem
  UNION ALL
  SELECT
    STRUCT ( TRUE AS indicador_licenciado,
      NULL AS indicador_ar_condicionado,
      NULL AS indicador_autuacao_ar_condicionado,
      NULL AS indicador_autuacao_seguranca,
      TRUE AS indicador_autuacao_limpeza,
      TRUE AS indicador_autuacao_equipamento,
      NULL AS indicador_sensor_temperatura,
      NULL AS indicador_validador_sbd ) AS indicadores,
    "Autuado por limpeza/equipamento" AS status,
    0 AS valor_km,
    DATE("2023-07-04") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 52820/2023" AS legislacao,
    4 AS ordem
  UNION ALL
  SELECT
    STRUCT ( TRUE AS indicador_licenciado,
      FALSE AS indicador_ar_condicionado,
      NULL AS indicador_autuacao_ar_condicionado,
      FALSE AS indicador_autuacao_seguranca,
      NULL AS indicador_autuacao_limpeza,
      NULL AS indicador_autuacao_equipamento,
      NULL AS indicador_sensor_temperatura,
      NULL AS indicador_validador_sbd ) AS indicadores,
    "Sem ar e não autuado" AS status,
    1.97 AS valor_km,
    DATE("2023-01-16") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 51889/2022" AS legislacao,
    5 AS ordem
  UNION ALL
  SELECT
    STRUCT ( TRUE AS indicador_licenciado,
      TRUE AS indicador_ar_condicionado,
      FALSE AS indicador_autuacao_ar_condicionado,
      FALSE AS indicador_autuacao_seguranca,
      NULL AS indicador_autuacao_limpeza,
      NULL AS indicador_autuacao_equipamento,
      NULL AS indicador_sensor_temperatura,
      NULL AS indicador_validador_sbd ) AS indicadores,
    "Com ar e não autuado" AS status,
    2.81 AS valor_km,
    DATE("2023-01-16") AS data_inicio,
    DATE("2023-12-31") AS data_fim,
    "DECRETO RIO 51889/2022" AS legislacao,
    6 AS ordem )
SELECT
  *
FROM
  valor_tipo_viagem
WHERE
  status != "Teste"