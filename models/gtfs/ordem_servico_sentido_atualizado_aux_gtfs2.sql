/*
  ordem_servico_gtfs2 com sentidos despivotados, ajustes nos horários e com atualização dos sentidos circulares
*/
{{
  config(
    materialized='ephemeral'
  )
}}

WITH
  -- 1. Identifica o sentido de cada serviço
  servico_trips_sentido AS (
    SELECT
      DISTINCT *
    FROM
      (
        SELECT
          feed_version, 
          trip_short_name AS servico,
          CASE
            WHEN ROUND(ST_Y(start_pt),4) = ROUND(ST_Y(end_pt),4) AND ROUND(ST_X(start_pt),4) = ROUND(ST_X(end_pt),4) THEN "C"
            WHEN direction_id = "0" THEN "I"
            WHEN direction_id = "1" THEN "V"
        END
          AS sentido
        FROM
          {{ ref("trips_filtrada_aux_gtfs2") }}
        WHERE
          indicador_trajeto_alternativo IS FALSE
      )   
    WHERE
      sentido = "C"
  ),
  -- 2. Busca principais informações na Ordem de Serviço (OS)
  ordem_servico AS (
    SELECT 
      * EXCEPT(horario_inicio, horario_fim),
      IF(horario_inicio IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_inicio, ":")) = 3, 
          PARSE_TIME("%T", 
                      CONCAT(
                          SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) AS INT64), 
                          ":", 
                          SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(1)] AS INT64), 
                          ":", 
                          SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(2)] AS INT64)
                      )
                    ), 
                    NULL
      ) AS inicio_periodo,
      IF(horario_fim IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_fim, ":")) = 3, 
          PARSE_TIME("%T", 
                      CONCAT(
                          SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) AS INT64), 
                          ":", 
                          SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(1)] AS INT64), 
                          ":", 
                          SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(2)] AS INT64)
                      )
                    ), 
                    NULL
      ) AS fim_periodo,
    FROM 
      {{ ref('ordem_servico_gtfs2') }}
    {% if is_incremental() -%}
      WHERE 
        feed_start_date = '{{ var("data_versao_gtfs") }}'
    {%- endif %}
  ),
  -- 3. Despivota ordem de serviço por sentido
  ordem_servico_sentido AS (
    SELECT
      *
    FROM
      ordem_servico
    UNPIVOT 
    (
      (
        distancia_planejada,
        partidas
      ) FOR sentido IN (
        (
          extensao_ida,
          partidas_ida
        ) AS "I",
        (
          extensao_volta,
          partidas_volta
        ) AS "V"
      )
    )
  )
  -- 4. Atualiza sentido dos serviços circulares na ordem de serviço
SELECT 
    o.* EXCEPT(sentido),
    COALESCE(s.sentido, o.sentido) AS sentido
FROM 
    ordem_servico_sentido AS o
LEFT JOIN
    servico_trips_sentido AS s
USING
    (feed_version, servico)
WHERE
    distancia_planejada != 0
    AND distancia_total_planejada != 0
    AND partidas != 0