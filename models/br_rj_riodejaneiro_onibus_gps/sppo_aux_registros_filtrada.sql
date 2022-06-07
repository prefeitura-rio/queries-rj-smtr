{{ 
  config(
      materialized='incremental',
      partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
      }
  )
}}
  /*
Descrição:
Filtragem e tratamento básico de registros de gps.
1. Filtra registros que estão fora de uma caixa que contém a área do município de Rio de Janeiro.
2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.
3. Muda o nome de variáveis para o padrão do projeto.
	- id_veiculo --> ordem
*/
WITH
box AS (
  /*1. Geometria de caixa que contém a área do município de Rio de Janeiro.*/ 
	SELECT
	*
	FROM
	{{ var('limites_caixa') }} 
),
gps AS (
  /*2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.*/ 
  SELECT
    *,
    ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
  FROM
    {{ ref('sppo_registros') }}
  {% if is_incremental() -%}
  WHERE
    data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
    AND timestamp_gps > "{{var('date_range_start')}}" and timestamp_gps <="{{var('date_range_end')}}"
    AND DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) BETWEEN 0 AND 1
  {%- endif -%}
),
filtrada AS (
  /*1,2, e 3. Muda o nome de variáveis para o padrão do projeto.*/
  SELECT
    ordem AS id_veiculo,
    latitude,
    longitude,
    posicao_veiculo_geo,
    velocidade,
    linha,
    timestamp_gps,
    timestamp_captura,
    data,
    hora,
    row_number() over (partition by ordem, timestamp_gps, linha) rn
  FROM
    gps
  WHERE
    ST_INTERSECTSBOX(posicao_veiculo_geo,
      ( SELECT min_longitude FROM box),
      ( SELECT min_latitude FROM box),
      ( SELECT max_longitude FROM box),
      ( SELECT max_latitude FROM box)) 
  )
SELECT
  * except(rn),
  {{ var("version") }} as versao
FROM
  filtrada
WHERE
  rn = 1