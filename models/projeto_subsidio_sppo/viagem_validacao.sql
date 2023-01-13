-- 1. Cria array com start_pt e end_pt por data, servico e sentido
WITH planejado AS (
  SELECT
    data,
    servico,
    sentido,
    ARRAY_AGG(end_pt) OVER (PARTITION BY servico, sentido) AS end_pt_agg,
    ARRAY_AGG(start_pt) OVER (PARTITION BY servico, sentido) AS start_pt_agg
  FROM
    {{ ref("viagem_planejada") }} ),
-- 2. Conta quantos start_pt e end_pt (total e distintos - eliminando duplicados)
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
-- 3. Cria ranking de viagens com mesmo id_veiculo e datetime_partida por distancia_planejada
  conformidade_dist AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_veiculo, datetime_partida ORDER BY distancia_planejada DESC) AS rn
    FROM
      {{ ref("viagem_conformidade") }}
  ),
-- 4. Cria ranking de viagens com mesmo id_veiculo e datetime_partida
  viagem_classificacao AS (
  SELECT
    c.* EXCEPT(rn),
    CASE
      -- Quando há dois ou mais end_pts de shapes para a mesma data, servico e sentido, mas a quantidade de end_pts distintos é diferente da quantidade total 
      -- (mais de um shape possui o mesmo end_pt) e, simultaneamente, a mesma lógica para os start_pts 
      -- Há mais de um shape com mesmo start_pt e end_pt, mas provavelmente há uma diferença ao longo da rota
      WHEN ((end_pt_n >= 2 AND end_pt_dist_n != end_pt_n) AND (start_pt_n >= 2 AND start_pt_dist_n != start_pt_n))
        THEN ROW_NUMBER() OVER (PARTITION BY id_veiculo, datetime_partida ORDER BY perc_conformidade_shape DESC, perc_conformidade_distancia DESC, perc_conformidade_registros DESC)
      ELSE
        -- Para os demais casos, considera-se a viagem com maior distancia_planejada
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
    -- Apenas para as duas viagens com shape mais extenso
    c.rn <= 2)
SELECT DISTINCT
  * EXCEPT(rn)
FROM
  viagem_classificacao
WHERE
  rn = 1