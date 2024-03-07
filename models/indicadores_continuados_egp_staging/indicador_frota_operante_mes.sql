{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
)}}

SELECT

  MIN(DATA) AS data,
  EXTRACT(YEAR FROM DATA) AS ano,
  EXTRACT(MONTH FROM DATA) AS mes,
  "Ônibus" AS modo,
  COUNT(DISTINCT id_veiculo) AS indicador_frota,
  
FROM
  {{ ref('viagem_completa') }}
GROUP BY
  1,2,3


