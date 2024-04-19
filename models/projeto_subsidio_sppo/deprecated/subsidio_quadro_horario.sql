SELECT 
    SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(servico AS STRING) servico,
    SAFE_CAST(vista AS STRING) vista,
    SAFE_CAST(consorcio AS STRING) consorcio,
    SAFE_CAST(horario_inicio AS TIME) horario_inicio,
    SAFE_CAST(horario_fim AS TIME) horario_fim,
    SAFE_CAST(trip_id AS STRING) trip_id,
    SAFE_CAST(sentido AS STRING) sentido,
    SAFE_CAST(distancia_planejada AS FLOAT64) distancia_planejada,
    SAFE_CAST(tipo_dia AS STRING) tipo_dia,
    SAFE_CAST(distancia_total_planejada AS FLOAT64) distancia_total_planejada
FROM {{ var("quadro_horario") }} AS t