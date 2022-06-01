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
    {{ var('sppo_registros') }}
  {%if is_incremental()%}
  {%set max_date = run_query('SELECT MAX(data) FROM' ~ this ).columns[0].values()[0]%}
  /* last_run_date configurada pra 1h antes da run, variações na periodicidade de materialização devem ser mudadas aqui*/
  {%set last_run_timestamp = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S") %}
  WHERE
    data >= "{{max_date}}"
    AND timestamp_gps >= "{{last_run_timestamp}}"
    AND DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) BETWEEN 0 AND 1
  {% endif %}
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
  -- STRUCT({{ maestro_sha }} AS versao_maestro,
  --   {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  filtrada
WHERE
  rn = 1