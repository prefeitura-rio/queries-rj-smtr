{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'data',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}
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
    FROM {{ ref('brt_aux_registros_filtrada') }}
    {% if is_incremental() -%}
    WHERE
      data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
      AND timestamp_gps > "{{var('date_range_start')}}" and timestamp_gps <="{{var('date_range_end')}}"
      AND DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) BETWEEN 0 AND 1
    {%- endif -%}
    ),
    velocidades AS (
    -- 2. velocidades
    SELECT
        id_veiculo, timestamp_gps, servico, velocidade, distancia, flag_em_movimento
    FROM {{ ref('brt_aux_registros_velocidade') }} 
    ),
    paradas as (
    -- 3. paradas
    SELECT 
        id_veiculo, timestamp_gps, servico, tipo_parada,
    FROM {{ ref('brt_aux_registros_parada') }}
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
    FROM {{ ref('brt_aux_registros_flag_trajeto_correto') }}
    )
-- 5. Junção final
SELECT
    "BRT" modo,
    r.timestamp_gps,
    date(r.timestamp_gps) data,
    extract(time from r.timestamp_gps) hora,
    r.id_veiculo,
    replace(r.servico, " ", "") as servico,
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
    "{{ var("version") }}" as versao
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
{% if is_incremental() -%}
  WHERE
  date(r.timestamp_gps) between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
  AND r.timestamp_gps > "{{var('date_range_start')}}" and r.timestamp_gps <="{{var('date_range_end')}}"
  AND DATETIME_DIFF(r.timestamp_captura, r.timestamp_gps, MINUTE) BETWEEN 0 AND 1
{%- endif -%}