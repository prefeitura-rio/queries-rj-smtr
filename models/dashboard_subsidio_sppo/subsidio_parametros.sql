{{
    config(
        materialized="table",
    )
}}

SELECT
  COALESCE(SAFE_CAST(indicador_licenciado AS BOOL), FALSE) indicador_licenciado,
  COALESCE(SAFE_CAST(indicador_ar_condicionado AS BOOL), FALSE) indicador_ar_condicionado,
  COALESCE(SAFE_CAST(indicador_autuacao_ar_condicionado AS BOOL), FALSE) indicador_autuacao_ar_condicionado,
  COALESCE(SAFE_CAST(indicador_autuacao_seguranca AS BOOL), FALSE) indicador_autuacao_seguranca,
  COALESCE(SAFE_CAST(indicador_autuacao_limpeza AS BOOL), FALSE) indicador_autuacao_limpeza,
  COALESCE(SAFE_CAST(indicador_autuacao_equipamento AS BOOL), FALSE) indicador_autuacao_equipamento,
  COALESCE(SAFE_CAST(indicador_sensor_temperatura AS BOOL), FALSE) indicador_sensor_temperatura,
  COALESCE(SAFE_CAST(indicador_validador_sbd AS BOOL), FALSE) indicador_validador_sbd,
  COALESCE(SAFE_CAST(indicador_registro_agente_verao_ar_condicionado AS BOOL), FALSE) indicador_registro_agente_verao_ar_condicionado,
  SAFE_CAST(status AS STRING) status,
  SAFE_CAST(subsidio_km AS FLOAT64) subsidio_km,
  SAFE_CAST(irk AS FLOAT64) irk,
  SAFE_CAST(data_inicio AS DATE) data_inicio,
  SAFE_CAST(data_fim AS DATE) data_fim,
  SAFE_CAST(legislacao AS STRING) legislacao,
  SAFE_CAST(ordem AS INT64) ordem
FROM
  {{ var("subsidio_parametros") }}