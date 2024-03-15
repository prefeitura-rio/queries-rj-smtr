{{ config(
  partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
  unique_key = ['servico', 'data_versao'],
  alias = 'ordem_servico'
) }}

WITH ordem_servico AS (
  SELECT SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(servico AS STRING) servico,
    SAFE_CAST(JSON_VALUE(content, '$.vista') AS STRING) vista,
    SAFE_CAST(JSON_VALUE(content, '$.consorcio') AS STRING) consorcio,
    SAFE_CAST(JSON_VALUE(content, '$.horario_inicio') AS STRING) horario_inicio,
    SAFE_CAST(JSON_VALUE(content, '$.horario_fim') AS STRING) horario_fim,
    SAFE_CAST(JSON_VALUE(content, '$.extensao_ida') AS FLOAT64) extensao_ida,
    SAFE_CAST(JSON_VALUE(content, '$.extensao_volta') AS FLOAT64) extensao_volta,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_ida_du') AS FLOAT64) AS INT64) partidas_ida_du,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_volta_du') AS FLOAT64) AS INT64) partidas_volta_du,
    SAFE_CAST(JSON_VALUE(content, '$.viagens_du') AS FLOAT64) viagens_du,
    SAFE_CAST(JSON_VALUE(content, '$.km_dia_util') AS FLOAT64) km_du,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_ida_pf') AS FLOAT64) AS INT64) partidas_ida_pf,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_volta_pf') AS FLOAT64) AS INT64) partidas_volta_pf,
    SAFE_CAST(JSON_VALUE(content, '$.viagens_pf') AS FLOAT64) viagens_pf,
    SAFE_CAST(JSON_VALUE(content, '$.km_pf') AS FLOAT64) km_pf,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_ida_sabado') AS FLOAT64) AS INT64) partidas_ida_sabado,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_volta_sabado') AS FLOAT64) AS INT64) partidas_volta_sabado,
    SAFE_CAST(JSON_VALUE(content, '$.viagens_sabado') AS FLOAT64) viagens_sabado,
    SAFE_CAST(JSON_VALUE(content, '$.km_sabado') AS FLOAT64) km_sabado,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_ida_domingo') AS FLOAT64) AS INT64) partidas_ida_domingo,
    SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.partidas_volta_domingo') AS FLOAT64) AS INT64) partidas_volta_domingo,
    SAFE_CAST(JSON_VALUE(content, '$.viagens_domingo') AS FLOAT64) viagens_domingo,
    SAFE_CAST(JSON_VALUE(content, '$.km_domingo') AS FLOAT64) km_domingo
    
  FROM {{ source(
      'br_rj_riodejaneiro_gtfs_staging',
      'ordem_servico'
    ) }}
  WHERE data_versao = '{{ var("data_versao_gtfs") }}')
SELECT *,
  '{{ var("version") }}' as versao_modelo
FROM ordem_servico UNPIVOT (
    (
      partidas_ida,
      partidas_volta,
      viagens_planejadas,
      distancia_total_planejada
    ) FOR tipo_dia IN (
      (
        partidas_ida_du,
        partidas_volta_du,
        viagens_du,
        km_du
      ) AS 'Dia Útil',
      (
        partidas_ida_pf,
        partidas_volta_pf,
        viagens_pf,
        km_pf
      ) AS 'Ponto Facultativo',
      (
        partidas_ida_sabado,
        partidas_volta_sabado,
        viagens_sabado,
        km_sabado
      ) AS 'Sabado',
      (
        partidas_ida_domingo,
        partidas_volta_domingo,
        viagens_domingo,
        km_domingo
      ) AS 'Domingo'
    )
  )
