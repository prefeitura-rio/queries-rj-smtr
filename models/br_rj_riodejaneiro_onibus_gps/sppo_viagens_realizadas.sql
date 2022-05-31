-- get start/end of trips, detects vehicles coming in the buffer at each boundary point
with 
    t as (
        SELECT 
            id_veiculo,
            linha_gps,
            linha_gtfs,
            round(distancia,1) distancia,
            trip_id, 
            faixa_horaria, 
            status,
            data, 
            hora, 
            string_agg(status,"") over (
                PARTITION BY id_veiculo, trip_id
                ORDER BY id_veiculo, trip_id, faixa_horaria
                ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) = 'startmiddle' starts,
            string_agg(status,"") over (
                PARTITION BY id_veiculo, trip_id
                ORDER BY id_veiculo, trip_id, faixa_horaria
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = 'middleend' ends
        FROM {{ intersec }}
        WHERE 
            data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }}) 
            AND linha_gps=linha_gtfs
        ORDER BY trip_id, faixa_horaria
    ),
    s AS (
        SELECT 
            *,
            CASE 
                WHEN
                string_agg(status,"") over (
                    PARTITION BY id_veiculo, trip_id
                    ORDER BY id_veiculo, trip_id, faixa_horaria
                    ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) = 'startend' 
                THEN faixa_horaria 
            END datetime_partida,
            CASE 
                WHEN string_agg(status,"") over (
                    PARTITION BY id_veiculo, trip_id
                    ORDER BY id_veiculo, trip_id, faixa_horaria
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = 'startend' 
                THEN faixa_horaria
            END datetime_chegada
        FROM 
            t
        WHERE 
            starts = true OR ends = true
    ),
    w AS (
        SELECT 
            * EXCEPT(datetime_partida), 
            lag(datetime_partida) over(
            PARTITION BY id_veiculo, trip_id 
            ORDER BY id_veiculo, trip_id, faixa_horaria) datetime_partida,
        FROM s
    ),
    realized_trips as (
        SELECT 
            id_veiculo,
            linha_gps,
            linha_gtfs,
            trip_id,
            datetime_partida,
            datetime_chegada,
            1 AS tipo_trajeto,
            datetime_diff(datetime_chegada, datetime_partida, minute) AS tempo_gasto, 
            round(SAFE_DIVIDE(distancia/1000, datetime_diff(datetime_chegada, datetime_partida, minute)/60), 1) AS velocidade_trajeto,
            STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
        FROM w
        WHERE datetime_partida IS NOT NULL
    )

SELECT 
    * 
FROM 
    realized_trips 
WHERE 
    velocidade_trajeto BETWEEN {{ filtro_min_velocidade }} AND {{ filtro_max_velocidade }}