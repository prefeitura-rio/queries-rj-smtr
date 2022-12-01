{{ config(
    materialized='incremental',
        partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    },
    unique_key=['data', 'trip_id'],
    incremental_strategy='insert_overwrite'
)
}}

-- 1. Obtém o serviço planejado do dia
WITH calendar AS (
  SELECT
    c.timestamp_captura,
    data,
    c.service_id
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(DATE("{{ var("run_date") }}"), DATE("{{ var("run_date") }}"))) AS data
  LEFT JOIN (
    SELECT *
    FROM {{ ref("calendar") }} AS c
    WHERE timestamp_captura = DATETIME("{{ var("gtfs_version") }}")
  ) AS c
  ON
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 1 AND c.sunday      = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 2 AND c.monday      = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 3 AND c.tuesday     = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 4 AND c.wednesday   = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 5 AND c.thursday    = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 6 AND c.friday      = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 7 AND c.saturday    = 1 THEN TRUE
        ELSE FALSE
      END
),
-- 1.1. Checa se há exceção para a data
calendar_dates AS (
    SELECT
        date, service_id
    FROM
        {{ ref("calendar_dates") }}
    WHERE
        timestamp_captura = DATETIME("{{ var("gtfs_version") }}")
        AND date = DATE("{{ var("run_date") }}")
        AND exception_type = 1 -- service added for the date
),
service_calendar AS (
    SELECT 
        c.* except(service_id),
        IFNULL(cd.service_id, c.service_id) as service_id
    FROM
        calendar c
    LEFT JOIN 
        calendar_dates cd
    ON c.data = cd.date
),
-- 2. Puxa informações do quadro horário com base no serviço planejado
quadro AS (
  SELECT
    * except(data, hora)
  FROM
    {{ ref("quadro") }}
  WHERE 
    timestamp_captura = DATETIME("{{ var("gtfs_version") }}")
),
combined AS (
    SELECT
        sc.data,
        q.*
    FROM
        service_calendar sc
    LEFT JOIN
        quadro q
    ON
        sc.service_id = q.service_id
),
-- 3. Puxa shape_id das trips
trips AS (
  SELECT
    trip_id, shape_id
  FROM
    {{ ref("trips") }}
  WHERE 
    timestamp_captura = DATETIME("{{ var("gtfs_version") }}")
),
-- 3. Puxa os shapes das trips planejadas
shapes AS (
    SELECT
        shape_id,
        SPLIT(shape_id, "_")[OFFSET(0)] AS shape_id_no_direction,
        SPLIT(shape_id, "_")[safe_offset(1)] AS shape_direction,
        shape,
        start_pt,
        end_pt
    FROM
        {{ ref("shapes_geom") }}
    WHERE
        timestamp_captura = DATETIME("{{ var("gtfs_version") }}")
),
-- 5. Agrega shapes ao quadro horario
shapes_quadro AS (
    SELECT
        c.*,
        st.shape,
        st.start_pt,
        st.end_pt,
        st.shape_id,
        st.shape_id_no_direction,
        st.shape_direction
    FROM
        combined c
    INNER JOIN (
        SELECT
            t.trip_id,
            s.*
        FROM
            trips t
        INNER JOIN
            shapes s
        ON
            t.shape_id = s.shape_id_no_direction
    ) AS st
    ON
        c.trip_id = st.trip_id
)
-- 6. Ajusta colunas finais
SELECT
    data,
    CASE 
        WHEN service_id = "U" THEN "Dia Útil"
        WHEN service_id = "S" THEN "Sabado"
        WHEN service_id = "D" THEN "Domingo"
    END AS tipo_dia,
    trip_short_name as servico,
    route_long_name as vista,
    agency_name as consorcio,
    CASE 
        WHEN REGEXP_CONTAINS(shape_id, "_") THEN "C"
        WHEN direction_id = 0 THEN "I"
        WHEN direction_id = 1 THEN "V"
    END AS sentido,
    shape_distance as distancia_planejada,
    trip_daily_distance as distancia_total_planejada,
    start_time as inicio_periodo,
    end_time as fim_periodo,
    trip_id as trip_id_planejado,
    CONCAT(trip_id, IFNULL(shape_direction, "")) AS trip_id,
    shape_id AS shape_id_planejado,
    shape_id_no_direction as shape_id,
    DATE(timestamp_captura) as data_shape,
    shape,
    CASE
        WHEN IFNULL(CAST(shape_direction AS INT64), direction_id) = 0 THEN "I"
        WHEN IFNULL(CAST(shape_direction AS INT64), direction_id) = 1 THEN "V"
    END AS sentido_shape,
    start_pt,
    end_pt
FROM
    shapes_quadro