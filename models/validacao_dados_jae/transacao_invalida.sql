{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data", 
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

WITH transacao AS (
  SELECT
    t.data,
    t.hora,
    t.datetime_transacao,
    t.datetime_processamento,
    t.datetime_captura,
    t.modo,
    t.id_consorcio,
    t.consorcio,
    t.id_operadora,
    t.operadora,
    t.id_servico_jae,
    s.servico,
    s.descricao_servico,
    t.id_transacao,
    t.longitude,
    t.latitude,
    IFNULL(t.longitude, 0) AS longitude_tratada,
    IFNULL(t.latitude, 0) AS latitude_tratada,
    s.longitude AS longitude_servico,
    s.latitude AS latitude_servico,
    s.id_servico_gtfs
  FROM
    {{ ref("transacao") }} t
  LEFT JOIN
    {{ ref("servicos") }} s
  USING (id_servico_jae)
  {% if is_incremental() %}
    WHERE 
      data = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
  {% endif %}
),
indicadores AS (
  SELECT
    * EXCEPT(id_servico_gtfs, latitude_tratada, longitude_tratada),
    latitude_tratada = 0 OR longitude_tratada = 0 AS indicador_geolocalizacao_zerada,
    (
      (latitude_tratada != 0 OR longitude_tratada != 0)
      AND NOT ST_INTERSECTSBOX(ST_GEOGPOINT(longitude_tratada, latitude_tratada), -43.87, -23.13, -43.0, -22.59)
    ) AS indicador_geolocalizacao_fora_rj,
    (
      (latitude_tratada != 0 OR longitude_tratada != 0)
      AND modo = "BRT"
      AND ST_DISTANCE(ST_GEOGPOINT(longitude_tratada, latitude_tratada), ST_GEOGPOINT(longitude_servico, latitude_servico)) > 100
    ) AS indicador_geolocalizacao_fora_stop,
    id_servico_gtfs IS NULL AND modo IN ("Ônibus", "BRT") AS indicador_servico_fora_gtfs
  FROM
    transacao
)
SELECT
  * EXCEPT(indicador_servico_fora_gtfs),
  CASE
    WHEN indicador_geolocalizacao_zerada = TRUE THEN "Geolocalização zerada"
    WHEN indicador_geolocalizacao_fora_rj = TRUE THEN "Geolocalização fora do município"
    WHEN indicador_geolocalizacao_fora_stop = TRUE THEN "Geolocalização fora do stop"
  END AS descricao_geolocalizacao_invalida,
  indicador_servico_fora_gtfs,
  '{{ var("version") }}' as versao
FROM
  indicadores
WHERE
  indicador_geolocalizacao_zerada = TRUE
  OR indicador_geolocalizacao_fora_rj = TRUE
  OR indicador_geolocalizacao_fora_stop = TRUE
  OR indicador_servico_fora_gtfs = TRUE