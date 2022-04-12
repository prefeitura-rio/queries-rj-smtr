-- get start/end of trips, detects vehicles coming in the buffer at each boundary point
with 
t as (
    SELECT 
        *,
        string_agg(status_viagem,"") over (
            PARTITION BY id_veiculo, shape_id
            ORDER BY id_veiculo, shape_id, timestamp_gps
            ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) = 'startmiddle' starts,
        string_agg(status_viagem,"") over (
            PARTITION BY id_veiculo, shape_id
            ORDER BY id_veiculo, shape_id, timestamp_gps
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = 'middleend' ends
    FROM `rj-smtr-dev.projeto_subsidio_sppo_v2.aux_registros_status_viagem`
    ORDER BY shape_id, timestamp_gps
),
s AS (
    SELECT 
        *,
        CASE 
            WHEN
            string_agg(status_viagem,"") over (
                PARTITION BY id_veiculo, shape_id
                ORDER BY id_veiculo, shape_id, timestamp_gps
                ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) = 'startend' 
            THEN timestamp_gps 
        END datetime_partida,
        CASE 
            WHEN string_agg(status_viagem,"") over (
                PARTITION BY id_veiculo, shape_id
                ORDER BY id_veiculo, shape_id, timestamp_gps
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = 'startend' 
            THEN timestamp_gps
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
        PARTITION BY id_veiculo, shape_id 
        ORDER BY id_veiculo, shape_id, timestamp_gps) datetime_partida,
    FROM s
),
realized_trips as (
    SELECT 
        *,
        row_number() over (
            partition by data, id_veiculo, servico, datetime_partida
            order by tempo_gasto
        ) rn
    FROM (
        SELECT 
            data,
            id_veiculo,
            servico,
            shape_id,
            datetime_partida,
            datetime_chegada,
            datetime_diff(datetime_chegada, datetime_partida, minute) AS tempo_gasto, 
            -- round(SAFE_DIVIDE(distancia/1000, datetime_diff(datetime_chegada, datetime_partida, minute)/60), 1) AS velocidade_trajeto,
            -- STRUCT("maestro_sha" AS versao_maestro, "maestro_bq_sha" AS versao_maestro_bq) versao
        FROM w
        WHERE datetime_partida IS NOT NULL
    ) 
)
SELECT r.* except(rn)
FROM realized_trips r
where rn = 1