WITH
  ordem_servico AS (
  SELECT
    SAFE_CAST(data_versao AS DATE) AS data_versao,
    timestamp_captura,
    servico,
    SAFE_CAST(JSON_VALUE(content, "$.vista") AS STRING) AS vista,
    SAFE_CAST(JSON_VALUE(content, "$.consorcio") AS STRING) AS consorcio,
    SAFE_CAST(JSON_VALUE(content, "$.horario_inicio") AS STRING) AS horario_inicio,
    SAFE_CAST(JSON_VALUE(content, "$.horario_fim") AS STRING) AS horario_fim,
    SAFE_CAST(JSON_VALUE(content, "$.extensao_ida") AS FLOAT64) AS extensao_ida,
    SAFE_CAST(JSON_VALUE(content, "$.extensao_volta") AS FLOAT64) AS extensao_volta,
    SAFE_CAST(JSON_VALUE(content, "$.partidas_ida_du") AS INT64) AS partidas_ida_du,
    SAFE_CAST(JSON_VALUE(content, "$.partidas_volta_du") AS INT64) AS partidas_volta_du,
    SAFE_CAST(JSON_VALUE(content, "$.viagens_du") AS FLOAT64) AS viagens_du,
    SAFE_CAST(JSON_VALUE(content, "$.km_dia_util") AS FLOAT64) AS km_du,
    SAFE_CAST(JSON_VALUE(content, "$.partidas_ida_pf") AS INT64) AS partidas_ida_pf,
    SAFE_CAST(JSON_VALUE(content, "$.partidas_volta_pf") AS INT64) AS partidas_volta_pf,
    SAFE_CAST(JSON_VALUE(content, "$.viagens_pf") AS FLOAT64) AS viagens_pf,
    SAFE_CAST(JSON_VALUE(content, "$.km_pf") AS FLOAT64) AS km_pf,
    NULL AS partidas_ida_sabado,
    NULL AS partidas_volta_sabado,
    SAFE_CAST(NULL AS FLOAT64) AS viagens_sabado,
    SAFE_CAST(JSON_VALUE(content, "$.km_sabado") AS FLOAT64) AS km_sabado,
    NULL AS partidas_ida_domingo,
    NULL AS partidas_volta_domingo,
    SAFE_CAST(NULL AS FLOAT64) AS viagens_domingo,
    SAFE_CAST(JSON_VALUE(content, "$.km_domingo") AS FLOAT64) AS km_domingo
  FROM
    {{ var("subsidio_ordem_servico") }} )
SELECT
  *
FROM
  ordem_servico UNPIVOT ((partidas_ida, partidas_volta, viagens_planejadas, distancia_total_planejada) 
  FOR tipo_dia IN ( (partidas_ida_du,       partidas_volta_du,      viagens_du,         km_du)      AS "Dia Útil",
                    (partidas_ida_pf,       partidas_volta_pf,      viagens_pf,         km_pf)      AS "Ponto Facultativo",
                    (partidas_ida_sabado,   partidas_volta_sabado,  viagens_sabado,     km_sabado)  AS "Sabado",
                    (partidas_ida_domingo,  partidas_volta_domingo, viagens_domingo,    km_domingo) AS "Domingo"))