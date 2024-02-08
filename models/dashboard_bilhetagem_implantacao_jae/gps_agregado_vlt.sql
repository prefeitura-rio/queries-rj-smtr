{{
  config(
    materialized="table",
  )
}}

WITH gps_agregado AS (
    SELECT
        data,
        servico,
        id_validador,
        latitude,
        longitude,
        estado_equipamento,
        primeiro_datetime_gps,
        ultimo_datetime_gps,
        TIMESTAMP_DIFF(
            ultimo_datetime_gps,
            primeiro_datetime_gps,
            MINUTE
        ) + 1 AS qtde_min_entre_a_prim_e_ultima_transmissao,
        COUNT(*) OVER (PARTITION BY servico, id_validador) AS qtde_registros_gps,
        COUNT(DISTINCT FORMAT_TIMESTAMP("%F %H:%M", datetime_gps)) OVER (PARTITION BY servico, id_validador) AS qtde_min_distintos_houve_transmissao,
        SUM(
            CASE 
                WHEN latitude != 0 AND longitude != 0 AND latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 
                ELSE 0 END
        ) OVER (PARTITION BY servico, id_validador) AS qtde_registros_gps_georreferenciados,
        ROW_NUMBER() OVER (PARTITION BY servico, id_validador ORDER BY datetime_gps) AS rn
    FROM
        (
            SELECT
                *,
                MIN(datetime_gps) OVER (PARTITION BY servico, id_validador) AS primeiro_datetime_gps,
                MAX(datetime_gps) OVER (PARTITION BY servico, id_validador) AS ultimo_datetime_gps,
                ROW_NUMBER() OVER (PARTITION BY id_transmissao_gps ORDER BY datetime_captura DESC) AS rn
            FROM
                {{ ref("gps_validador") }}
            WHERE
                data = current_date("America/Sao_Paulo")
                AND modo = "VLT"
        )
    WHERE
        rn = 1
)
SELECT
    servico,
    id_validador,
    latitude,
    longitude,
    data,
    estado_equipamento,
    primeiro_datetime_gps,
    ultimo_datetime_gps,
    qtde_min_entre_a_prim_e_ultima_transmissao,
    qtde_min_distintos_houve_transmissao,
    qtde_registros_gps,
    qtde_registros_gps_georreferenciados,
    IFNULL(SAFE_DIVIDE(qtde_registros_gps_georreferenciados, qtde_registros_gps), 0) AS percentual_registros_gps_georreferenciados,
    IFNULL(SAFE_DIVIDE(qtde_min_distintos_houve_transmissao, qtde_min_entre_a_prim_e_ultima_transmissao), 0) AS percentual_transmissao_a_cada_min
FROM
    gps_agregado
WHERE
    rn = 1