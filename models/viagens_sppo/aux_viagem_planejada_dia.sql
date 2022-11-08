-- Calculo número de viagens total por dia util

-- 1 Calcula numero de viagens por periodo
WITH frequencies_viagens AS (
  SELECT
    * EXCEPT (exact_times),
    case
      when end_time < start_time
      THEN (time_diff("23:59:59", start_time, second) + time_diff(end_time, "00:00:00", second) + 1) / headway_secs
      else time_diff(end_time, start_time, second) / headway_secs
    end as viagens_planejadas
  FROM `rj-smtr-dev.gtfs_test.frequencies`

),
-- 2. Realiza join com trips
trips_viagens AS (
  SELECT
    f.* except(trip_id),
    SPLIT(t.trip_id, '_')[SAFE_ORDINAL(1)] AS trip_id,
    t.trip_short_name,
    t.route_id,
    --t.service_id,
    t.direction_id,
    t.shape_id
  FROM frequencies_viagens AS f
  LEFT JOIN `rj-smtr-dev.gtfs_test.trips` AS t
  ON
    t.trip_id = f.trip_id
  AND
    t.data_versao = f.data_versao
)
-- 3. Agrega horarios de trips para um dia completo
SELECT
  data_versao,
  trip_short_name,
  route_id,
  --service_id,
  direction_id,
  shape_id,
  SUM(viagens_planejadas) AS viagens_planejadas
FROM trips_viagens
GROUP BY 1, 2, 3, 4, 5
order by viagens_planejadas