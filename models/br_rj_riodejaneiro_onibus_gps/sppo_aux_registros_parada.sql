/*
Descrição:
Identifica veículos parados em terminais ou garagens conhecidas.
1. Selecionamos os terminais conhecidos e uma geometria do tipo polígono (Polygon) que contém buracos nas
localizações das garagens.
2. Calculamos as distâncias do veículos em relação aos terminais conhecidos. Definimos aqui a coluna 'nrow',
que identifica qual o terminal que está mais próximo do ponto informado. No passo final, recuperamos apenas 
os dados com nrow = 1 (menor distância em relação à posição do veículo)
3. Definimos uma distancia_limiar_parada. Caso o veículo esteja a uma distância menor que este valor de uma
parada, será considerado como parado no terminal com menor distancia.
4. Caso o veiculo não esteja intersectando o polígono das garagens, ele será considerado como parado dentro
de uma garagem (o polígono é vazado nas garagens, a não intersecção implica em estar dentro de um dos 'buracos').
*/
WITH 
  terminais as (
    -- 1. Selecionamos terminais, criando uma geometria de ponto para cada.
    select
      ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_terminal nome_parada, 'terminal' tipo_parada
    from {{ terminais }}
  ),
  garagem_polygon AS (
    -- 1. Selecionamos o polígono das garagens.
    SELECT  ST_GEOGFROMTEXT(WKT,make_valid => true) AS poly
    FROM {{ polygon_garagem }}
  ),
  distancia AS (
    --2. Calculamos as distâncias e definimos nrow
    SELECT 
      id_veiculo, 
      timestamp_gps,
      data, 
      linha, 
      posicao_veiculo_geo, 
      nome_parada, 
      tipo_parada,
      ROUND(ST_DISTANCE(posicao_veiculo_geo, ponto_parada), 1) distancia_parada,
      ROW_NUMBER() OVER (PARTITION BY timestamp_gps, id_veiculo ORDER BY ST_DISTANCE(posicao_veiculo_geo, ponto_parada)) nrow
    FROM terminais p
    JOIN (
      SELECT 
        id_veiculo, 
        timestamp_gps,
        data, 
        linha, 
        posicao_veiculo_geo
      FROM  
        {{ registros_filtrada }}
      WHERE
        data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
      AND
        timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
      ) r
    on 1=1
  )
SELECT
  data,
  id_veiculo,
  timestamp_gps,
  linha,
  /*
  3. e 4. Identificamos o status do veículo como 'terminal', 'garagem' (para os veículos parados) ou 
  null (para os veículos mais distantes de uma parada que o limiar definido)
  */
  case
    when distancia_parada < {{ distancia_limiar_parada }} then tipo_parada
    when not ST_INTERSECTS(posicao_veiculo_geo, (SELECT  poly FROM garagem_polygon)) then 'garagem'
    else null
  end tipo_parada,
FROM distancia
WHERE nrow = 1