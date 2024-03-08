{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'month' },
)}}

SELECT

  MIN(data) AS data,
  EXTRACT(YEAR FROM data) AS ano,
  EXTRACT(MONTH FROM data) AS mes,
  "Ônibus" AS modo,
  COUNT(DISTINCT id_veiculo) AS indicador_frota,
  
FROM
  {{ ref('viagem_completa') }}
GROUP BY
  2,3


