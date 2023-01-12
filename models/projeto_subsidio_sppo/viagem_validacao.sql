WITH planejado AS (
  SELECT
    data,
    servico,
    sentido,
    ARRAY_AGG(end_pt) OVER (PARTITION BY servico, sentido) AS end_pt_agg,
    ARRAY_AGG(start_pt) OVER (PARTITION BY servico, sentido) AS start_pt_agg
  FROM
    {{ ref("viagem_planejada") }} ),
  planejado_count AS (
    SELECT
      * EXCEPT(end_pt_agg, start_pt_agg),
      ARRAY_LENGTH(end_pt_agg) AS end_pt_n,
      ARRAY_LENGTH(ARRAY(
      SELECT
        DISTINCT ST_GEOHASH(p)
      FROM
        UNNEST(end_pt_agg) AS p)) AS end_pt_dist_n,
      ARRAY_LENGTH(start_pt_agg) AS start_pt_n,
      ARRAY_LENGTH(ARRAY(
      SELECT
        DISTINCT ST_GEOHASH(p)
      FROM
        UNNEST(start_pt_agg) AS p)) AS start_pt_dist_n
    FROM
      planejado ),
  conformidade_dist AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_veiculo, datetime_partida ORDER BY distancia_planejada DESC) AS rn
    FROM
      {{ ref("viagem_conformidade") }}
  ),
  viagem_classificacao AS (
  SELECT
    c.* EXCEPT(rn),
    CASE
      WHEN ((end_pt_n >= 2 AND end_pt_dist_n != end_pt_n) AND (start_pt_n >= 2 AND start_pt_dist_n != start_pt_n))
        THEN ROW_NUMBER() OVER (PARTITION BY id_veiculo, datetime_partida ORDER BY perc_conformidade_shape DESC, perc_conformidade_distancia DESC, perc_conformidade_registros DESC)
      ELSE
        c.rn
    END AS rn
  FROM
    conformidade_dist AS c
  LEFT JOIN
    planejado_count AS p
  ON
    c.servico_realizado = p.servico
    AND c.sentido = p.sentido
    AND c.data = p.data
  WHERE
    c.rn <= 2)
SELECT DISTINCT
  * EXCEPT(rn)
FROM
  viagem_classificacao
WHERE
  rn = 1