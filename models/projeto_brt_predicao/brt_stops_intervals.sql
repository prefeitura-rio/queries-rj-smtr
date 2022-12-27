{# 1. Verifica os timestamps que estao num raio de 400m do ponto => stop, stop_sequence, timestamp, distancia
2. Pega o que tem a menor distancia para cada ponto + id_veiculo
3. Determina ponto de origem e destino em relacao a cada stop identificado e o tempo de chegada/saida
4. Remove outliters (hardlimit) #}

-- 1. Verifica os timestamps que estao num raio de 400m do ponto
WITH
  stops AS (
  SELECT
    stop_id,
    ST_GEOGPOINT(stop_lon, stop_lat) AS stop_point,
    timestamp_captura AS data_versao
  FROM
    `rj-smtr-dev.gtfs.stops`
  WHERE
    timestamp_captura = "2022-12-27 10:46:00" ),
  stop_times AS (
  SELECT
    trip_id,
    stop_id,
    stop_sequence,
    shape_dist_traveled
  FROM
    `rj-smtr-dev.gtfs.stop_times`
  WHERE
    timestamp_captura = "2022-12-27 10:46:00"
    AND trip_id = "19dabe0f-a321-4a7c-a177-16b779c3c919"), -- TESTE
  sequence AS (
  SELECT
    st.*,
    s.* EXCEPT(stop_id)
  FROM
    stops s
  INNER JOIN
    stop_times st
  ON
    s.stop_id = st.stop_id ),
  gps_sequence AS (
  SELECT
    r.*,
    s.* EXCEPT(trip_id),
    ST_DISTANCE(posicao_veiculo_geo, stop_point) AS stop_distance
  FROM
    `rj-smtr-dev.projeto_brt_previsao.aux_brt_registros_trip` r
  INNER JOIN
    sequence s
  ON
    s.trip_id = r.trip_id
  WHERE
    ST_DWITHIN(posicao_veiculo_geo, stop_point, 250 ) ),
  -- 2. Pega o que tem a menor distancia para cada ponto + id_veiculo de acordo com a sequencia da trip
  gps_stops_diff AS (
  SELECT
    id_veiculo,
    -- trip_id,
    stop_id,
    stop_sequence,
    timestamp_gps,
    CASE
      WHEN stop_sequence = LAG(stop_sequence) OVER (PARTITION BY id_veiculo, trip_id ORDER BY timestamp_gps) THEN 0
    ELSE
    1
  END
    AS diff
  FROM
    gps_sequence
  ORDER BY
    id_veiculo,
    timestamp_gps )
SELECT
  g.* EXCEPT(diff),
  SUM(diff) OVER (ORDER BY 1) AS gps_partition -- TODO: corrigir grupos
FROM
  gps_stops_diff g
