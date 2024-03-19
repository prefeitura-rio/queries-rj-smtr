{{
  config(
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    }
  )
}}

{% if execute %}
  {% set feed_start_date_query %}
    SELECT 
      MAX(feed_start_date) as last_feed_start_date
    FROM 
      rj-smtr.gtfs.feed_info
    WHERE
      feed_start_date < DATE("{{ var( 'run_date' ) }}")
  {% endset %}
  
  {% set last_feed_start_date = run_query(feed_start_date_query)[0].values()[0] %}
{% endif %}

with cte_service_ids_today AS (
  SELECT 
    service_id
  FROM 
    rj-smtr.gtfs.calendar
  WHERE 
    ( DATE("{{ var( 'run_date' ) }}") BETWEEN start_date AND end_date)
    AND (
        (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 2 AND monday = "1")
        OR (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 3 AND tuesday = "1")
        OR (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 4 AND wednesday = "1")
        OR (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 5 AND thursday = "1")
        OR (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 6 AND friday = "1")
        OR (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 7 AND saturday = "1")
        OR (EXTRACT(DAYOFWEEK FROM  DATE("{{ var( 'run_date' ) }}") ) = 1 AND sunday = "1")
      )
    AND feed_start_date = "{{ last_feed_start_date }}"
),
cte_trips_today AS (
  SELECT 
    t.trip_id,
    t.service_id,
    r.route_id,
    route_short_name,
    t.direction_id,
    f.headway_secs,
    DATETIME_ADD(
      DATETIME_ADD(
        DATETIME_ADD(
          DATE_TRUNC(DATETIME("{{ var( 'run_date' ) }}"), DAY), 
          INTERVAL CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64) HOUR
        ), 
        INTERVAL CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64) MINUTE
      ), 
      INTERVAL CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) 
    AS datetime_trip_start,
    DATETIME_ADD(
      DATETIME_ADD(
        DATETIME_ADD(
          DATE_TRUNC(DATETIME("{{ var( 'run_date' ) }}"), DAY), 
          INTERVAL CAST(SPLIT(end_time, ':')[OFFSET(0)] AS INT64) HOUR
        ), 
        INTERVAL CAST(SPLIT(end_time, ':')[OFFSET(1)] AS INT64) MINUTE
      ), 
      INTERVAL CAST(SPLIT(end_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) 
    AS datetime_trip_end,
  FROM 
    rj-smtr.gtfs.trips AS t 
    JOIN rj-smtr.gtfs.frequencies AS f ON t.trip_id = f.trip_id 
    JOIN rj-smtr.gtfs.routes AS r ON t.route_id = r.route_id 
    JOIN cte_service_ids_today AS sit ON t.service_id = sit.service_id
  WHERE 
    t.feed_start_date = "{{ last_feed_start_date }}" 
    AND f.feed_start_date = "{{ last_feed_start_date }}"
    AND r.feed_start_date = "{{ last_feed_start_date }}"
    AND t.direction_id = "0"
    AND r.route_short_name = "457"
),
cte_planned_trips_timestamp_array AS (
  SELECT
    trip_id,
    service_id,
    route_id,
    route_short_name,
    direction_id,
    GENERATE_TIMESTAMP_ARRAY(
      CAST(datetime_trip_start AS TIMESTAMP),
      DATETIME_SUB(CAST(datetime_trip_end AS TIMESTAMP), INTERVAL 1 SECOND),
      INTERVAL headway_secs SECOND
    ) AS datetime_trip_start_array
  FROM
    cte_trips_today
  WHERE
    datetime_trip_start BETWEEN 
    DATE_TRUNC(DATE("{{ var( 'run_date' ) }}"), DAY) 
    AND DATETIME_ADD(DATE_TRUNC(DATE("{{ var( 'run_date' ) }}"), DAY), INTERVAL 1 DAY )
),
cte_planned_trips_datetime_expanded AS (
  SELECT
    trip_id,
    service_id,
    route_id,
    route_short_name,
    direction_id, 
    CAST(timestamp_trip_start AS DATETIME) AS datetime_trip_start
  FROM
    cte_planned_trips_timestamp_array,
    UNNEST(datetime_trip_start_array) AS timestamp_trip_start
),
cte_trip_stops AS (
    SELECT 
        st.trip_id,
        st.stop_id,
        ST_GEOGPOINT(stop_lon, stop_lat) AS stop_location,
        stop_name,
        arrival_time
    FROM 
      rj-smtr.gtfs.stop_times AS st
      JOIN cte_trips_today AS tt ON st.trip_id = tt.trip_id
      JOIN rj-smtr.gtfs.stops AS s ON st.stop_id = s.stop_id
    WHERE 
        st.feed_start_date = "{{ last_feed_start_date }}"
        AND s.feed_start_date = "{{ last_feed_start_date }}"
),
cte_final AS (
  SELECT 
    "{{ last_feed_start_date }}" AS feed_start_date,
    DATE("{{ var( 'run_date' ) }}") AS data,
    ptde.service_id,
    ptde.route_id,
    ptde.route_short_name,
    ptde.trip_id,
    ptde.direction_id,
    datetime_trip_start,
    st.stop_id,
    DATETIME_ADD(
      DATETIME_ADD(
        DATETIME_ADD(
          datetime_trip_start, 
          INTERVAL CAST(SPLIT(arrival_time, ':')[OFFSET(0)] AS INT64) HOUR
        ), 
        INTERVAL CAST(SPLIT(arrival_time, ':')[OFFSET(1)] AS INT64) MINUTE
      ), 
      INTERVAL CAST(SPLIT(arrival_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) 
    AS datetime_stop_arrival,
    stop_name,
    stop_location
FROM 
  cte_planned_trips_datetime_expanded AS ptde
  JOIN cte_trip_stops AS st ON ptde.trip_id = st.trip_id
ORDER BY 
  ptde.trip_id,
  datetime_stop_arrival
)

SELECT * FROM cte_final

