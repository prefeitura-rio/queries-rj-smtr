/*
Descrição:
Junção dos passos de tratamento, junta as informações extras que definimos a partir dos registros
capturados.
Para descrição detalhada de como cada coluna é calculada, consulte a documentação de cada uma das tabelas
utilizadas abaixo.
1. registros_filtrada: filtragem e tratamento básico dos dados brutos capturados.
2. aux_registros_velocidade: estimativa da velocidade de veículo a cada ponto registrado e identificação
do estado de movimento ('parado', 'andando')
3. aux_registros_parada: identifica veículos parados em terminais ou garagens conhecidas
4. aux_registros_flag_trajeto_correto: calcula intersecções das posições registradas para cada veículo
com o traçado da linha informada.
5. As junções (joins) são feitas sobre o id_veículo e a timestamp_gps.
*/
WITH
    registros as (
    -- 1. registros_filtrada
    SELECT 
        id_veiculo,
        timestamp_gps,
        timestamp_captura,
        velocidade,
        servico,
        latitude,
        longitude,
    FROM {{ brt_registros_filtrada }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    ),
    velocidades AS (
    -- 2. velocidades
    SELECT
        id_veiculo, timestamp_gps, servico, velocidade, distancia, flag_em_movimento
    FROM {{ brt_velocidade }} 
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    ),
    paradas as (
    -- 3. paradas
    SELECT 
        id_veiculo, timestamp_gps, servico, tipo_parada,
    FROM {{ brt_parada }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    ),
    flags AS (
    -- 4. flag_trajeto_correto
    SELECT
        id_veiculo,
        timestamp_gps, 
        servico,
        route_id, 
        flag_linha_existe_sigmob,
        flag_trajeto_correto, 
        flag_trajeto_correto_hist
    FROM {{ brt_flag_trajeto_correto }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    )
-- 5. Junção final
SELECT
    "BRT" modo,
    r.timestamp_gps,
    date(r.timestamp_gps) data,
    extract(time from r.timestamp_gps) hora,
    r.id_veiculo,
    r.servico servico,
    r.latitude,
    r.longitude,
    CASE 
        WHEN 
        flag_em_movimento IS true AND flag_trajeto_correto_hist is true
        THEN true
    ELSE false
    END flag_em_operacao,
    v.flag_em_movimento,
    p.tipo_parada,
    flag_linha_existe_sigmob,
    flag_trajeto_correto,
    flag_trajeto_correto_hist,
    CASE
        WHEN flag_em_movimento IS true AND flag_trajeto_correto_hist is true
        THEN 'Em operação'
        WHEN flag_em_movimento is true and flag_trajeto_correto_hist is false
        THEN 'Operando fora trajeto'
        WHEN flag_em_movimento is false
        THEN 
            CASE
                WHEN tipo_parada is not null
                THEN concat("Parado ", tipo_parada)
            ELSE
                CASE
                    WHEN flag_trajeto_correto_hist is true
                    THEN 'Parado trajeto correto'
                ELSE 'Parado fora trajeto'
                END
            END
    END status,
    r.velocidade velocidade_instantanea,
    v.velocidade velocidade_estimada_10_min,
    v.distancia,
    STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
    registros r

JOIN
    flags f
ON
    r.id_veiculo = f.id_veiculo
    AND r.timestamp_gps = f.timestamp_gps
    AND r.servico = f.servico

JOIN
    velocidades v
ON
    r.id_veiculo = v.id_veiculo
    AND  r.timestamp_gps = v.timestamp_gps
    AND  r.servico = v.servico

JOIN 
    paradas p
ON  
    r.id_veiculo = p.id_veiculo
    AND  r.timestamp_gps = p.timestamp_gps
    AND r.servico = p.servico 