{{ 
  config(
    partition_by = {
      "field": "feed_start_date",
      "data_type": "date",
      "granularity": "day"
    },
    alias = "ordem_servico_trips_shapes"
  )
}}

{% if execute -%}
  {% if is_incremental() -%}
    {%- set query = "SELECT DISTINCT evento FROM " ~ ref('ordem_servico_trajeto_alternativo_gtfs2') ~ " WHERE feed_start_date = '" ~ var('data_versao_gtfs')  ~ "'" -%}
  {% else %}
    {%- set query = "SELECT DISTINCT evento FROM " ~ ref('ordem_servico_trajeto_alternativo_gtfs2') -%}
  {% endif -%}
  {%- set eventos_trajetos_alternativos = run_query(query).columns[0].values() -%}
{% endif %}

WITH 
  -- 1. Busca os shapes em formato geográfico
  shapes AS (
    SELECT
      *
    FROM
      {{ ref("shapes_geom_gtfs2") }}
    {% if is_incremental() -%}
    WHERE 
      feed_start_date = '{{ var("data_versao_gtfs") }}'
    {% endif -%}
  ),
  -- 2. Busca as trips
  trips AS (
    SELECT
      *
    FROM
    (
      SELECT
        * EXCEPT(rn),
          CASE
            WHEN service_id LIKE "%U_%" THEN "Dia Útil"
            WHEN service_id LIKE "%S_%" THEN "Sabado"
            WHEN service_id LIKE "%D_%" THEN "Domingo"
          ELSE
          NULL
        END
          AS tipo_dia
      FROM
      -- 3. Busca as trips de referência para cada serviço, sentido, e tipo_dia
      (
        SELECT
          *,
          FALSE AS indicador_trajeto_alternativo
        FROM 
        (
          SELECT
            service_id,
            trip_id,
            trip_headsign,
            trip_short_name,
            direction_id,
            shape_id,
            feed_version,
            ROW_NUMBER() OVER (PARTITION BY feed_version, trip_short_name, service_id, direction_id ORDER BY feed_version, trip_short_name, service_id, shape_id, direction_id) AS rn
          FROM
            {{ ref("trips_gtfs2") }}
          WHERE
            {% if is_incremental() -%}
            feed_start_date = '{{ var("data_versao_gtfs") }}' AND
            {% endif %}
            {% for evento in eventos_trajetos_alternativos %}
              trip_headsign NOT LIKE "%{{evento}}%" -- Desconsidera trajetos alternativos
              {% if not loop.last %}AND{% endif %}
            {% endfor %}
            AND service_id NOT LIKE "%_DESAT_%"  -- Desconsidera service_ids desativados
            AND service_id != "EXCEP"  -- Desconsidera service_id com trajetos alternativos
          )
        WHERE
          rn = 1
      )
      UNION ALL
      (
        SELECT
          *,
          TRUE AS indicador_trajeto_alternativo
        FROM
        -- 4. Busca as trips de referência dos trajetos alternativos para cada serviço e sentido
        (
          SELECT
            service_id,
            trip_id,
            trip_headsign,
            trip_short_name,
            direction_id,
            shape_id,
            feed_version,
            ROW_NUMBER() OVER (PARTITION BY feed_version, trip_short_name, service_id, shape_id, direction_id ORDER BY feed_version, trip_short_name, service_id, shape_id, direction_id) AS rn
          FROM
            {{ ref("trips_gtfs2") }}
          WHERE
            {% if is_incremental() -%}
            feed_start_date = '{{ var("data_versao_gtfs") }}' AND
            {% endif %}
            (
            {% for evento in eventos_trajetos_alternativos %}
              trip_headsign LIKE "%{{evento}}%" -- Considera trajetos alternativos
              {% if not loop.last %}OR{% endif %}
            {% endfor %}
            )
            AND service_id NOT LIKE "%_DESAT_%"  -- Desconsidera service_ids desativados
          )
      WHERE
        rn = 1 
      )
    )
  ),
  -- 4. Identifica o sentido de cada serviço
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
          trips
        LEFT JOIN
          shapes
        USING
          (feed_version, 
            shape_id)
        WHERE
          indicador_trajeto_alternativo IS FALSE
      )   
    WHERE
      sentido = "C"
  ),
  -- 5. Busca principais informações na Ordem de Serviço (OS)
  ordem_servico AS (
    SELECT 
      * EXCEPT(horario_inicio, horario_fim),
      IF(horario_inicio IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_inicio, ":")) = 3, 
          PARSE_TIME("%T", 
                      CONCAT(
                          CAST(MOD(CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) AS STRING), 
                          ":", 
                          SPLIT(horario_inicio, ":")[OFFSET(1)], 
                          ":", 
                          SPLIT(horario_inicio, ":")[OFFSET(2)]
                      )
                    ), 
                    NULL
      ) AS inicio_periodo,
      IF(horario_fim IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_fim, ":")) = 3, 
          PARSE_TIME("%T", 
                      CONCAT(
                          CAST(MOD(CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) AS STRING), 
                          ":", 
                          SPLIT(horario_fim, ":")[OFFSET(1)], 
                          ":", 
                          SPLIT(horario_fim, ":")[OFFSET(2)]
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
  -- 7. Despivota ordem de serviço por sentido
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
  ),
  -- 8. Atualiza sentido dos serviços circulares na ordem de serviço
  ordem_servico_sentido_atualizado AS (
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
  ),
  -- 9. Busca anexo de trajetos alternativos
  ordem_servico_trajeto_alternativo AS (
    SELECT 
      *
    FROM 
      {{ ref("ordem_servico_trajeto_alternativo_gtfs2") }}
    {% if is_incremental() -%}
      WHERE 
        feed_start_date = "{{ var('data_versao_gtfs') }}"
    {%- endif %}
  ),
  -- 10. Despivota anexo de trajetos alternativos
  ordem_servico_trajeto_alternativo_sentido AS (
    SELECT
      *
    FROM
      ordem_servico_trajeto_alternativo
    UNPIVOT 
    (
      (
        distancia_planejada
      ) FOR sentido IN (
        (
          extensao_ida
        ) AS "I",
        (
          extensao_volta
        ) AS "V"
      )
    )
  ),
  -- 11. Atualiza sentido dos serviços circulares no anexo de trajetos alternativos
  ordem_servico_trajeto_alternativo_sentido_atualizado AS (
  SELECT
    * EXCEPT(sentido),
    CASE
      WHEN "C" IN UNNEST(sentido_array) THEN "C"
      ELSE o.sentido
    END AS sentido,
  FROM
    ordem_servico_trajeto_alternativo_sentido AS o
  LEFT JOIN
    (
      SELECT
        feed_start_date,
        servico,
        ARRAY_AGG(DISTINCT sentido) AS sentido_array,
      FROM
        ordem_servico_sentido_atualizado
      GROUP BY
        1,
        2
    ) AS s
  USING
    (feed_start_date, servico)
  WHERE
    distancia_planejada != 0
  ),
  -- 12. Trata a OS, inclui trip_ids e ajusta nomes das colunas
  ordem_servico_tratada AS (
  SELECT
    *
  FROM
    (
      (
      SELECT
        o.feed_version,
        o.feed_start_date,
        o.feed_end_date,
        o.tipo_dia,
        servico,
        vista,
        consorcio,
        sentido,
        distancia_planejada,
        distancia_total_planejada,
        inicio_periodo,
        fim_periodo,
        trip_id,
        shape_id,
      FROM
        ordem_servico_sentido_atualizado AS o
      LEFT JOIN
        trips AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND 
          (o.tipo_dia = t.tipo_dia
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil"))
        AND
          ((o.sentido IN ("I", "C") AND t.direction_id = "0")
          OR (o.sentido = "V" AND t.direction_id = "1"))
      WHERE
        indicador_trajeto_alternativo IS FALSE
      )
    UNION ALL
      (
      SELECT
        o.feed_version,
        o.feed_start_date,
        o.feed_end_date,
        o.tipo_dia,
        servico,
        o.vista || " " || ot.evento AS vista,
        o.consorcio,
        sentido,
        ot.distancia_planejada,
        distancia_total_planejada,
        COALESCE(ot.inicio_periodo, o.inicio_periodo) AS inicio_periodo,
        COALESCE(ot.fim_periodo, o.fim_periodo) AS fim_periodo,
        trip_id,
        shape_id,
      FROM
        ordem_servico_trajeto_alternativo_sentido_atualizado AS ot
      LEFT JOIN
        ordem_servico_sentido_atualizado AS o
      USING
        (feed_version,
          servico,
          sentido)
      LEFT JOIN
        trips AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND 
          (o.tipo_dia = t.tipo_dia
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil"))
        AND
          ((o.sentido IN ("I", "C") AND t.direction_id = "0")
          OR (o.sentido = "V" AND t.direction_id = "1"))
        AND t.trip_headsign LIKE CONCAT("%", ot.evento, "%")
      WHERE
        indicador_trajeto_alternativo IS TRUE
        AND trip_id IS NOT NULL -- Remove serviços de tipo_dia sem planejamento
      )
    )
  ),
  -- 13. Inclui trip_ids de ida e volta para trajetos circulares e ajusta shape_id para trajetos circulares
  ordem_servico_trips AS (
  SELECT
    * EXCEPT(shape_id),
    shape_id AS shape_id_planejado,
    CASE
      WHEN sentido = "C" THEN shape_id || "_" || SPLIT(trip_id, "_")[OFFSET(1)]
    ELSE
    shape_id
  END
    AS shape_id,
  FROM (
    SELECT
      DISTINCT * EXCEPT(trip_id),
      trip_id AS trip_id_planejado,
      trip_id
    FROM
      ordem_servico_tratada
    WHERE
      sentido = "I"
      OR sentido = "V" )
  UNION ALL (
    SELECT
      * EXCEPT(trip_id),
      trip_id AS trip_id_planejado,
      CONCAT(trip_id, "_0") AS trip_id,
    FROM
      ordem_servico_tratada
    WHERE
      sentido = "C" )
  UNION ALL (
    SELECT
      * EXCEPT(trip_id),
      trip_id AS trip_id_planejado,
      CONCAT(trip_id, "_1") AS trip_id,
    FROM
      ordem_servico_tratada
    WHERE
      sentido = "C" ) )
SELECT
  feed_version,
  feed_start_date,
  o.feed_end_date,
  tipo_dia,
  servico,
  vista,
  consorcio,
  sentido,
  distancia_planejada,
  distancia_total_planejada,
  inicio_periodo,
  fim_periodo,
  trip_id_planejado,
  trip_id,
  shape_id,
  shape_id_planejado,
  shape,
  CASE
    WHEN sentido = "C" AND SPLIT(shape_id, "_")[OFFSET(1)] = "0" THEN "I"
    WHEN sentido = "C" AND SPLIT(shape_id, "_")[OFFSET(1)] = "1" THEN "V"
    WHEN sentido = "I" OR sentido = "V" THEN sentido
END
  AS sentido_shape,
  start_pt,
  end_pt,
FROM
  ordem_servico_trips AS o
LEFT JOIN
  shapes
USING
  (feed_version,
    feed_start_date,
    shape_id)
