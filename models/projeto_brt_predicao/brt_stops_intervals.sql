  -- 1. Verifica os timestamps que estao num raio de 400m do ponto
WITH
  stops AS (
  SELECT
    stop_id,
    ST_GEOGPOINT(stop_lon, stop_lat) AS stop_point,
    -- timestamp_captura AS data_versao
  FROM
    `rj-smtr-dev.gtfs.stops`
  WHERE
    timestamp_captura = "2022-12-27 10:46:00" ),
  stop_times AS (
  SELECT
    trip_id,
    stop_id,
    stop_sequence,
    -- shape_dist_traveled
  FROM
    `rj-smtr-dev.gtfs.stop_times`
  WHERE
    timestamp_captura = "2022-12-27 10:46:00"),
  stops_sequence AS (
  SELECT
    st.trip_id,
    st.stop_id,
    st.stop_sequence,
    -- st.shape_dist_traveled,
    s.stop_point,
  FROM
    stops s
  INNER JOIN
    stop_times st
  ON
    s.stop_id = st.stop_id ),
  cross_gps_stops AS (
  SELECT
    r.id_veiculo,
    r.timestamp_gps,
    -- r.posicao_veiculo_geo,
    -- r.trip_short_name,
    -- r.direction_id,
    s.trip_id,
    s.stop_id,
    s.stop_sequence,
    -- st.shape_dist_traveled,
    s.stop_point,
    ST_DISTANCE(posicao_veiculo_geo, stop_point) AS stop_distance
  FROM
    `rj-smtr-dev.projeto_brt_previsao.aux_brt_registros_trip` r
  INNER JOIN
    stops_sequence s
  ON
    s.trip_id = r.trip_id
  WHERE
    ST_DWITHIN(posicao_veiculo_geo, stop_point, 250 ) ),
  -- 2. Deduplica timestamps proximos de +1 ponto escolhendo o mais proximo
  gps_closest_stop AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_veiculo, timestamp_gps ORDER BY stop_distance) AS rn
    FROM
      cross_gps_stops)
  WHERE
    rn = 1
  ORDER BY
    id_veiculo,
    timestamp_gps ),
  -- 3. Agrupa as posições do veículo inclusas no raio de cada ponto e escolhe a mais próxima
  aux_gps_partition AS (
  SELECT
    * EXCEPT(diff),
    SUM(diff) OVER (PARTITION BY 1 ORDER BY id_veiculo, timestamp_gps) AS gps_partition
  FROM (
    SELECT
      *,
      CASE
        WHEN stop_sequence = LAG(stop_sequence) OVER (PARTITION BY id_veiculo, trip_id ORDER BY timestamp_gps) THEN 0
      ELSE
      1
    END
      AS diff
    FROM
      gps_closest_stop )
  ORDER BY
    id_veiculo,
    timestamp_gps ),
  gps_stop_times AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY gps_partition ORDER BY stop_distance) AS rn
    FROM
      aux_gps_partition )
  WHERE
    rn = 1
  ORDER BY
    id_veiculo,
    timestamp_gps )
  -- 4. Calcula os intervalos de origem/destino
SELECT
  EXTRACT(DAYOFWEEK
  FROM
    timestamp_gps) AS tipo_dia,
  EXTRACT(hour
  FROM
    timestamp_gps) AS hora,
  id_veiculo,
  trip_id,
  stop_id AS stop_id_origin,
  LEAD(stop_id) OVER (PARTITION BY id_veiculo ORDER BY gps_partition) AS stop_id_destiny,
  stop_sequence AS stop_sequence_origin,
  LEAD(stop_sequence) OVER (PARTITION BY id_veiculo ORDER BY gps_partition) AS stop_sequence_destiny,
  DATETIME_DIFF(LEAD(timestamp_gps) OVER (PARTITION BY id_veiculo ORDER BY gps_partition), timestamp_gps, second) AS stop_interval
FROM
  gps_stop_times
ORDER BY
  id_veiculo,
  timestamp_gps
