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
    * EXCEPT(id_servico_gtfs),
    CASE
      WHEN
        latitude = 0 OR latitude IS NULL OR longitude = 0 OR longitude IS NULL THEN "Geolocalização zerada"
      WHEN
        ST_INTERSECTSBOX(ST_GEOGPOINT(longitude, latitude), -43.87, -23.13, -43.0, -22.59) = FALSE THEN "Geolocalização fora do município"
      WHEN
        modo = "BRT" AND ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(longitude_servico, latitude)) > 90 THEN "Geolocalização fora do stop"
    END AS tipo_transacao_invalida,
    id_servico_gtfs IS NOT NULL AS indicador_servico_gtfs
  FROM
    transacao
)
SELECT
  *,
  '{{ var("version") }}' as versao
FROM
  indicadores
WHERE
  tipo_transacao_invalida IS NOT NULL
  OR indicador_servico_gtfs = FALSE