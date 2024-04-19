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
  -- 2. Trata a OS, inclui trip_ids e ajusta nomes das colunas
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
        o.tipo_os,
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
        indicador_trajeto_alternativo
      FROM
        {{ ref("ordem_servico_sentido_atualizado_aux_gtfs2") }} AS o
      LEFT JOIN
        {{ ref("trips_filtrada_aux_gtfs2") }} AS t
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
        o.tipo_os,
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
        indicador_trajeto_alternativo
      FROM
        {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs2") }} AS ot
      LEFT JOIN
        {{ ref("ordem_servico_sentido_atualizado_aux_gtfs2") }} AS o
      USING
        (feed_version,
          servico,
          sentido)
      LEFT JOIN
        {{ ref("trips_filtrada_aux_gtfs2") }} AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND 
          (o.tipo_dia = t.tipo_dia
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil")
          OR (t.tipo_dia = "EXCEP")) -- Inclui trips do service_id/tipo_dia "EXCEP"
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
  -- 3. Inclui trip_ids de ida e volta para trajetos circulares, ajusta shape_id para trajetos circulares e inclui id_tipo_trajeto
  ordem_servico_trips AS (
    SELECT
      * EXCEPT(shape_id, indicador_trajeto_alternativo),
      shape_id AS shape_id_planejado,
      CASE
        WHEN sentido = "C" THEN shape_id || "_" || SPLIT(trip_id, "_")[OFFSET(1)]
      ELSE
      shape_id
    END
      AS shape_id,
      CASE
        WHEN indicador_trajeto_alternativo IS FALSE THEN 0 -- Trajeto regular
        WHEN indicador_trajeto_alternativo IS TRUE THEN 1 -- Trajeto alternativo
    END
      AS id_tipo_trajeto,
    FROM 
    (
      (
        SELECT
          DISTINCT * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          trip_id
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "I"
          OR sentido = "V" 
      )
      UNION ALL 
      (
        SELECT
          * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          CONCAT(trip_id, "_0") AS trip_id,
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "C" 
      )
      UNION ALL 
      (
        SELECT
          * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          CONCAT(trip_id, "_1") AS trip_id,
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "C" 
      ) 
    )
  )
SELECT
  feed_version,
  feed_start_date,
  o.feed_end_date,
  tipo_os,
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
  s.start_pt,
  s.end_pt,
  id_tipo_trajeto,
FROM
  ordem_servico_trips AS o
LEFT JOIN
  shapes AS s
USING
  (feed_version,
    feed_start_date,
    shape_id)
