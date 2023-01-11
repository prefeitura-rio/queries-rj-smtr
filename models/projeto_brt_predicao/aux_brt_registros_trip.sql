{# Filtra dados históricos de GPS e adiciona identificação de sentido e trip #}

WITH
  gps AS (
  SELECT
    id_veiculo,
    IFNULL(REGEXP_EXTRACT(servico, r'[0-9]+'), "") AS trip_short_name,
    -- TODO: deve vir somente o nuemro da linha, sem correção do nosso lado
    CASE
      WHEN sentido LIKE "ITI IDA%" THEN 0
      WHEN sentido LIKE "ITI VOLTA%" THEN 1
  END
    AS direction_id,
    timestamp_gps,
    ST_GEOGPOINT(longitude, latitude) AS posicao_veiculo_geo
  FROM
    `rj-smtr.br_rj_riodejaneiro_brt_gps.registros_desaninhada`
  WHERE
    DATA BETWEEN "2022-12-01"
    AND "2022-12-02"),
  trips AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      trip_id,
      trip_short_name,
      direction_id,
      timestamp_captura AS data_versao,
      ROW_NUMBER() OVER (PARTITION BY trip_short_name, direction_id ORDER BY trip_id) AS rn
    FROM
      `rj-smtr-dev.gtfs.trips`
    WHERE
      timestamp_captura = "2022-12-27 10:46:00"
      AND direction_id IS NOT NULL )
  WHERE
    rn = 1) -- TODO: atualizar gtfs para incluir sentido da 52
SELECT
  g.*,
  t.trip_id
FROM
  gps g
INNER JOIN
  trips t
ON
  g.trip_short_name = t.trip_short_name
  AND g.direction_id = t.direction_id
