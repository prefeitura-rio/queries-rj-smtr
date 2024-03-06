{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
)}}

SELECT

  MIN(DATA) AS data,
  EXTRACT(YEAR FROM DATA) AS ano,
  EXTRACT(MONTH FROM DATA) AS mes,
  servico_informado AS servico,
  COUNT(DISTINCT id_veiculo) AS indicador_frota,
  
FROM
  `rj-smtr.projeto_subsidio_sppo.viagem_completa`
GROUP BY
  1,2,3


