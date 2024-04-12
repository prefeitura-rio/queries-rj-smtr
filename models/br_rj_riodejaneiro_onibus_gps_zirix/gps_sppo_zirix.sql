{{
  config(
    materialized='incremental',
    partition_by={
      'field':"data",
      'data_type':'date',
      'granularity': 'day'
    },
    alias='gps_sppo'
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
      linha,
      latitude,
      longitude,
    FROM {{ ref('sppo_aux_registros_filtrada_zirix') }}
    {% if is_incremental() -%}
    WHERE
      data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
      AND timestamp_gps > "{{var('date_range_start')}}" and timestamp_gps <="{{var('date_range_end')}}"
    {%- endif -%}
  ),
  velocidades AS (
    -- 2. velocidades
    SELECT
      id_veiculo, timestamp_gps, linha, velocidade, distancia, flag_em_movimento
    FROM
      {{ ref('sppo_aux_registros_velocidade_zirix') }}
  ),
  paradas as (
    -- 3. paradas
    SELECT 
      id_veiculo, timestamp_gps, linha, tipo_parada,
    FROM {{ ref('sppo_aux_registros_parada_zirix') }}
  ),
  flags AS (
    -- 4. flag_trajeto_correto
    SELECT
      id_veiculo,
      timestamp_gps, 
      linha,
      route_id, 
      flag_linha_existe_sigmob,
      flag_trajeto_correto, 
      flag_trajeto_correto_hist
    FROM
      {{ ref('sppo_aux_registros_flag_trajeto_correto_zirix') }}
  )
-- 5. Junção final
SELECT
  "SPPO" modo,
  r.timestamp_gps,
  date(r.timestamp_gps) data,
  extract(time from r.timestamp_gps) hora, 
  r.id_veiculo,
  r.linha as servico,
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
  AND r.linha = f.linha

JOIN
  velocidades v
ON
  r.id_veiculo = v.id_veiculo
  AND  r.timestamp_gps = v.timestamp_gps
  AND  r.linha = v.linha

JOIN 
  paradas p
ON  
  r.id_veiculo = p.id_veiculo
  AND  r.timestamp_gps = p.timestamp_gps
  AND r.linha = p.linha
{% if is_incremental() -%}
  WHERE
  date(r.timestamp_gps) between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
  AND r.timestamp_gps > "{{var('date_range_start')}}" and r.timestamp_gps <="{{var('date_range_end')}}"
{%- endif -%}
