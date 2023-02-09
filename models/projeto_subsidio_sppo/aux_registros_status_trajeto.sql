  -- 1. Seleciona sinais de GPS registrados NO período
WITH
  gps AS (
  SELECT
    g.* EXCEPT(longitude, latitude),
    SUBSTR(id_veiculo, 2, 3) AS id_empresa,
    ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
  FROM
    `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g -- {{ ref('gps_sppo') }} g
  WHERE
    ( data BETWEEN DATE("{{ var("run_date") }}") AND DATE_ADD(DATE("{{ var("run_date") }}"), INTERVAL 1 DAY) )
    -- Limita range de busca do gps de D0 às 00h até D+1 às 3h
    AND ( timestamp_gps BETWEEN DATETIME_TRUNC("{{ var("run_date") }}", day)
     AND DATETIME_ADD(DATETIME_TRUNC("{{ var("run_date") }}", day), INTERVAL 27 hour) )
    AND status != "Parado garagem" ),
  -- 2. Classifica a posição DO veículo em todos os shapes possíveis de 
  -- serviços de uma mesma empresa 
  status_viagem AS (
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
    s.start_pt,
    s.end_pt,
    s.distancia_planejada,
    IFNULL(g.distancia, 0) AS distancia,
    CASE
      WHEN ST_DWITHIN(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }}) THEN 'start'
      WHEN ST_DWITHIN(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }}) THEN 'end'
      WHEN ST_DWITHIN(g.posicao_veiculo_geo, shape, {{ var("buffer") }}) THEN 'middle'
    ELSE
    'out'
  END
    status_viagem
  FROM
    gps g
  INNER JOIN (
    SELECT
      *
    FROM
      {{ ref("viagem_planejada") }}
    WHERE
      data BETWEEN DATE("{{ var("run_date") }}") AND DATE_ADD(DATE("{{ var("run_date") }}"), INTERVAL 1 DAY) ) s
  ON
    g.data = s.data
    AND g.servico = s.servico )
SELECT
  *,
  '{{ var("version") }}' AS versao_modelo
FROM
  status_viagem