-- 1. Seleciona sinais de GPS registrados no período
WITH gps AS (
    SELECT 
        g.* EXCEPT(longitude, latitude),
        SUBSTR(id_veiculo, 2, 3) AS id_empresa,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
    FROM  
        `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g -- {{ ref('gps_sppo') }} g
    WHERE (
        data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
        -- data = '2022-10-01'
    )
    Limita range de busca do gps de D-2 às 00h até D-1 às 3h
    and (
        timestamp_gps between datetime_sub(datetime_trunc("{{ var("run_date") }}", day), interval 1 day)
        and datetime_add(datetime_trunc("{{ var("run_date") }}", day), interval 3 hour)
    )
    and status != 'Parado garagem'
),
-- -- 2. Separa shapes/trips circulares em ida/volta
aux_viagem_planejada_circ AS (
    SELECT
        data_versao,
        trip_short_name,
        route_id,
        direction_id,
        shape_id,
        sentido,
        service_id,
        shape,
        start_pt,
        end_pt,
        sentido AS sentido_shape,
        distancia_planejada
    FROM (
        SELECT
            *
        FROM 
            `rj-smtr-dev.viagens.viagem_planejada` AS v
        WHERE 
            (sentido = 'I' OR sentido = 'V')
        AND
            data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
            -- v.data_versao = '2022-10-01'
    )
    UNION ALL (
        SELECT
            v.data_versao,
            v.trip_short_name,
            v.route_id,
            v.direction_id,
            v.shape_id,
            v.sentido,
            v.service_id,
            c.shape_ida AS shape,
            v.start_pt,
            c.mid_pt AS end_pt,
            "I" AS sentido_shape,
            c.shape_distance_ida AS distancia_planejada
        FROM 
            `rj-smtr-dev.viagens.viagem_planejada` AS v
        JOIN
            `rj-smtr-dev.gtfs_test.aux_circ_shapes_geom` AS c
        ON
            v.shape_id = c.shape_id
        AND
            v.data_versao = c.data_versao
        WHERE   
            (
                v.sentido = 'C'
            AND
                data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
                -- v.data_versao = '2022-10-01'
            )
    )
    UNION ALL (
        SELECT
            v.data_versao,
            v.trip_short_name,
            v.route_id,
            v.direction_id,
            v.shape_id,
            v.sentido,
            v.service_id,
            c.shape_volta AS shape,
            c.mid_pt AS start_pt,
            v.end_pt,
            "V" AS sentido_shape,
            c.shape_distance_volta AS distancia_planejada
        FROM 
            `rj-smtr-dev.viagens.viagem_planejada` AS v
        JOIN
            `rj-smtr-dev.gtfs_test.aux_circ_shapes_geom` AS c
        ON
            v.shape_id = c.shape_id
        AND
            v.data_versao = c.data_versao
        WHERE   
            (
                v.sentido = 'C'
            AND
                data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
                -- v.data_versao = '2022-10-01'
            )
    )
),
-- 2. Classifica a posição do veículo em todos os shapes possíveis de
--    serviços de uma mesma empresa
status_viagem AS (
    SELECT
        g.data,
        g.id_veiculo,
        g.id_empresa,
        g.timestamp_gps,
        TIMESTAMP_TRUNC(g.timestamp_gps, minute) AS timestamp_minuto_gps,
        g.posicao_veiculo_geo,
        TRIM(g.servico, " ") AS servico_informado,
        v.trip_short_name AS servico_realizado,
        v.shape_id,
        v.sentido_shape,
        v.sentido,
        v.start_pt,
        v.end_pt,
        v.distancia_planejada,
        IFNULL(g.distancia,0) AS distancia,
        CASE
            WHEN ST_DWITHIN(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }})
            THEN 'start'
            WHEN ST_DWITHIN(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }})
            THEN 'end'
            WHEN ST_DWITHIN(g.posicao_veiculo_geo, shape, {{ var("buffer") }})
            THEN 'middle'
        ELSE 'out'
        END AS status_viagem
    FROM  
        gps AS g
    LEFT JOIN `rj-smtr-dev.viagens.aux_data_versao` AS d
    ON 
        g.data = d.data
    LEFT JOIN aux_viagem_planejada_circ AS v
    ON 
        d.service_id = v.service_id
    AND
        g.servico = v.trip_short_name
    AND
        data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
        -- v.data_versao = '2022-10-01'

)

SELECT 
    *
FROM  
    status_viagem