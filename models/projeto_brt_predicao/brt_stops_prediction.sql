{# Calcula a predição de chegada no próximo ponto para todas as trips #}

SELECT
  DISTINCT stop_id_origin,
  stop_id_destiny,
  tipo_dia,
  hora,
  PERCENTILE_CONT(stop_interval, 0.5) OVER(PARTITION BY stop_id_origin, stop_id_destiny, tipo_dia, hora) AS delta_secs
FROM
 {{ ref("brt_stops_intervals") }}
