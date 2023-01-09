WITH
  gps AS (
  SELECT
    g.* EXCEPT(longitude, latitude),
    SUBSTR(id_veiculo, 2, 3) AS id_empresa,
    ST_GEOGPOINT(longitude, latitude) AS posicao_veiculo_geo
  FROM
    `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` AS g -- `rj-smtr-dev`.`br_rj_riodejaneiro_veiculos`.`gps_sppo` g
  WHERE
    ( DATA BETWEEN DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 day)
      AND DATE("{{ var("run_date") }}") )
    -- Limita RANGE de busca do gps de D-2 às 00h ATÉ D-1 às 3h
    AND ( timestamp_gps BETWEEN DATETIME_SUB(DATETIME_TRUNC("{{ var("run_date") }}", day), INTERVAL 1 day)
      AND DATETIME_ADD(DATETIME_TRUNC("{{ var("run_date") }}", day), INTERVAL 3 hour) )
    AND status != "Parado garagem" ),
  gps_shape AS (
  SELECT
    g.data,
    g.id_veiculo,
    g.id_empresa,
    g.timestamp_gps,
    TIMESTAMP_TRUNC(g.timestamp_gps, minute) AS timestamp_minuto_gps,
    g.posicao_veiculo_geo,
    TRIM(g.servico, " ") AS servico_informado,
    s.servico AS servico_realizado,
    s.shape_id,
    s.sentido_shape,
    s.shape_id_planejado,
    s.trip_id,
    s.trip_id_planejado,
    s.sentido,
    -- s.start_pt,
    -- s.end_pt,
    -- s.distancia_planejada,
    IFNULL(g.distancia,0) AS distancia
  FROM
    gps AS g
  INNER JOIN (
    SELECT
      *
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_planejada` -- `rj-smtr-dev`.`projeto_subsidio_sppo_844_conf1`.`viagem_planejada`
    WHERE
      DATA BETWEEN DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 day)
      AND DATE("{{ var("run_date") }}") ) AS s
  ON
    g.data = s.data
    AND g.servico = s.servico
  ),
  distance_shape AS (
  SELECT
    g.data,
    g.id_veiculo,
    g.timestamp_gps,
    g.shape_id,
    -- Calcula a distância percorrida no shape
    ST_LENGTH(ST_DUMP(ST_DIFFERENCE(s.shape, ST_BUFFER(ST_CLOSESTPOINT(s.shape, g.posicao_veiculo_geo), 1)))[ORDINAL(1)]) AS distancia_perc,
    ST_LENGTH(s.shape) AS distancia_total
  FROM
    gps_shape AS g
  INNER JOIN (
    SELECT
      *
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_planejada` -- `rj-smtr-dev`.`projeto_subsidio_sppo_844_conf1`.`viagem_planejada`
    WHERE
      DATA BETWEEN DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 day)
      AND DATE("{{ var("run_date") }}")
      AND sentido = 'C' ) AS s
  ON
    (
      ST_DWITHIN(g.posicao_veiculo_geo, s.start_pt, {{ var("buffer") }})
      OR ST_DWITHIN(g.posicao_veiculo_geo, s.end_pt, {{ var("buffer") }})
      )
    AND g.data = s.data
    AND g.shape_id = s.shape_id
  ),
  join_distance AS (
  SELECT
    g.*,
    d.distancia_perc,
    d.distancia_total
  FROM
    gps_shape AS g
  LEFT JOIN
    distance_shape AS d
  ON
    g.data = d.data
    AND g.shape_id = d.shape_id
    AND g.id_veiculo = d.id_veiculo
    AND g.timestamp_gps = d.timestamp_gps
  )

SELECT
  j.* EXCEPT(distancia_perc, distancia_total, distancia),
  s.start_pt,
  s.end_pt,
  s.distancia_planejada,
  j.distancia,
  CASE
    WHEN (s.sentido = 'C') THEN (
      -- Método novo
      CASE
        -- Caso a distância percorrida no shape seja inferior a 500 m
        WHEN j.distancia_perc < {{ var("buffer") }} THEN 'start'
        -- Caso a distância a ser percorrida no shape seja inferior a 500 m
        WHEN (j.distancia_total - j.distancia_perc) < {{ var("buffer") }} THEN 'end'
        -- Caso a distância percorrida no shape seja igual ou superior a 500 m e a distância a ser percorrida no shape seja igual ou superior a 500 m
        WHEN ST_DWITHIN(j.posicao_veiculo_geo, s.shape, {{ var("buffer") }}) THEN 'middle'
      ELSE 'out'
      END
    )
  ELSE (
      CASE
        WHEN ST_DWITHIN(j.posicao_veiculo_geo, s.start_pt, {{ var("buffer") }}) THEN 'start'
        WHEN ST_DWITHIN(j.posicao_veiculo_geo, s.end_pt, {{ var("buffer") }}) THEN 'end'
        WHEN ST_DWITHIN(j.posicao_veiculo_geo, s.shape, {{ var("buffer") }}) THEN 'middle'
      ELSE 'out'
    END
  )
  END AS status_viagem,
  '{{ var("version") }}' as versao_modelo
FROM
  join_distance AS j
INNER JOIN (
  SELECT
    *
  FROM
    `rj-smtr.projeto_subsidio_sppo.viagem_planejada` -- `rj-smtr-dev`.`projeto_subsidio_sppo_844_conf1`.`viagem_planejada`
  WHERE
    DATA BETWEEN DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 day)
    AND DATE("{{ var("run_date") }}") ) AS s
ON
  j.data = s.data
  AND j.shape_id = s.shape_id