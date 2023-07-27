/* Criar a tabela para ser usada no dashboard sobre GPS 8 dias*/

WITH
  gtfs AS (
  SELECT DISTINCT 
    route_short_name 
  FROM 
    `rj-smtr.br_rj_riodejaneiro_sigmob.routes_desaninhada` 
  WHERE 
    agency_id IN ('22002', '22003', '22004', '22005')),
  gps AS (
  SELECT
    id_veiculo,
    servico,
    latitude,
    longitude,
    DATE(timestamp_gps) AS data,
    TIME(timestamp_gps) AS hora,
    timestamp_gps
  FROM
    rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo
  WHERE
    data between date_sub(current_date(), interval 8 day) and CURRENT_DATE() )
SELECT
  gps.*
FROM
  gps
JOIN
  gtfs
ON gps.servico = route_short_name 


