{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(servico AS STRING) as servico,
    SAFE_CAST(vista AS STRING) as vista,
    SAFE_CAST(consorcio AS STRING) as consorcio,
    SAFE_CAST(horario_inicio AS STRING) as horario_inicio,
    SAFE_CAST(horario_fim AS STRING) as horario_fim,
    SAFE_CAST(extensao_ida AS FLOAT64) as extensao_ida,
    SAFE_CAST(extensao_volta AS FLOAT64) as extensao_volta,
    SAFE_CAST(partidas_ida_dia_util AS INT64) as partidas_ida_dia_util,
    SAFE_CAST(partidas_volta_dia_util AS INT64) as partidas_volta_dia_util,
    SAFE_CAST(viagens_dia_util AS INT64) as viagens_dia_util,
    SAFE_CAST(quilometragem_dia_util AS FLOAT64) as quilometragem_dia_util,
    SAFE_CAST(partidas_ida_sabado AS INT64) as partidas_ida_sabado,
    SAFE_CAST(partidas_volta_sabado AS INT64) as partidas_volta_sabado,
    SAFE_CAST(viagens_sabado AS INT64) as viagens_sabado,
    SAFE_CAST(quilometragem_sabado AS FLOAT64) as quilometragem_sabado,
    SAFE_CAST(partidas_ida_domingo AS INT64) as partidas_ida_domingo,
    SAFE_CAST(partidas_volta_domingo AS INT64) as partidas_volta_domingo,
    SAFE_CAST(viagens_domingo AS INT64) as viagens_domingo,
    SAFE_CAST(quilometragem_domingo AS FLOAT64) as quilometragem_domingo
FROM 
  `rj-smtr-dev.gtfs_teste_leone.os`
