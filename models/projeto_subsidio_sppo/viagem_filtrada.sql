-- 1. Filtra viagens com mesma chegada e partida pelo maior % de conformidade do shape - total: 25798
WITH filtro_desvio as (
  SELECT
  * EXCEPT(rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id_veiculo, datetime_partida, datetime_chegada ORDER BY perc_conformidade_shape DESC) AS rn
  FROM
    {{ ref("viagem_conformidade") }} )
WHERE
  rn = 1
),
-- 2. Filtra viagens com partida ou chegada diferentes pela maior distancia percorrida -  total: 23289
filtro_partida AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY id_veiculo, datetime_partida ORDER BY distancia_planejada DESC) AS rn
    FROM
      filtro_desvio )
  WHERE
    rn = 1 ) 
-- filtro_chegada
SELECT
  * EXCEPT(rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id_veiculo, datetime_chegada ORDER BY distancia_planejada DESC) AS rn
  FROM
    filtro_partida )
WHERE
  rn = 1
