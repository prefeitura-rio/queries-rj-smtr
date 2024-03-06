{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
)}}

SELECT
  MIN(DATA) AS data,
  EXTRACT(YEAR FROM DATA) AS ano,
  EXTRACT(MONTH FROM DATA) AS mes,
  ROUND(AVG(idade),2) AS indicador_idade_media
FROM (
  SELECT
    *,
    EXTRACT(YEAR
    FROM
      CURRENT_DATE())-SAFE_CAST(ano_fabricacao AS INT64) AS idade
  FROM
    `rj-smtr.veiculo.sppo_licenciamento` )
GROUP BY
  1,
  2, 
  3
