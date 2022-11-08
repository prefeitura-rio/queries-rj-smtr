-- 1. Cria colunas identificadoras de início (starts) e fim (ends) de viagens
WITH aux_status AS (
    SELECT 
        *,
        STRING_AGG(status_viagem,"") OVER (
            PARTITION BY id_veiculo, shape_id
            ORDER BY id_veiculo, shape_id, timestamp_gps
            ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) = 'startmiddle' AS starts,
        STRING_AGG(status_viagem,"") OVER (
            PARTITION BY id_veiculo, shape_id
            ORDER BY id_veiculo, shape_id, timestamp_gps
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = 'middleend' AS ends
    FROM 
        --{{ ref('aux_registros_status_trajeto') }} 
        `rj-smtr-dev.viagens.aux_registros_status_trajeto` AS r
),
-- 2. Classifica início-fim consecutivos como partida-chegada da viagem
aux_inicio_fim AS (
    SELECT 
        *,
        CASE 
            WHEN
            STRING_AGG(status_viagem,"") OVER (
                PARTITION BY id_veiculo, shape_id
                ORDER BY id_veiculo, shape_id, timestamp_gps
                ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) = 'startend' 
            THEN timestamp_gps 
        END AS datetime_partida,
        CASE 
            WHEN STRING_AGG(status_viagem,"") OVER (
                PARTITION BY id_veiculo, shape_id
                ORDER BY id_veiculo, shape_id, timestamp_gps
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = 'startend' 
            THEN timestamp_gps
        END AS datetime_chegada
    FROM 
        aux_status
    WHERE 
        starts = TRUE OR ends = TRUE
),
-- 3. Junta partida-chegada da viagem na mesma linha e cria campo identificador da viagem (id_viagem)
inicio_fim AS (
    SELECT 
        * EXCEPT(datetime_chegada, posicao_veiculo_geo),
        posicao_veiculo_geo AS posicao_partida,
        LEAD(datetime_chegada) OVER(
            PARTITION BY id_veiculo, shape_id 
            ORDER BY id_veiculo, shape_id, timestamp_gps
        ) AS datetime_chegada,
        LEAD(posicao_veiculo_geo) OVER(
            PARTITION BY id_veiculo, shape_id 
            ORDER BY id_veiculo, shape_id, timestamp_gps
        ) AS posicao_chegada,
    FROM aux_inicio_fim
)
-- 4. Filtra colunas e cria campo identificador da viagem (id_viagem)
SELECT DISTINCT
    CONCAT(id_veiculo, "-", servico_realizado ,"-", sentido, "-", FORMAT_DATETIME("%Y%m%d%H%M%S", datetime_partida)) as id_viagem,
    data,
    id_empresa,
    id_veiculo,
    servico_informado, -- no momento da partida
    servico_realizado,
    --trip_id,
    shape_id,
    sentido_shape,
    --round((st_distance(start_pt, posicao_partida) + st_distance(end_pt, posicao_chegada))/1000, 3) as distancia_inicio_fim,
    (ST_DISTANCE(start_pt, posicao_partida) + ST_DISTANCE(end_pt, posicao_chegada)) AS distancia_inicio_fim,
    distancia_planejada,
    --shape_id_planejado,
    --trip_id_planejado,
    sentido,
    datetime_partida,
    datetime_chegada,
    --'{{ var("version") }}' AS versao_modelo
FROM 
    inicio_fim AS i
WHERE 
     i.datetime_partida IS NOT NULL