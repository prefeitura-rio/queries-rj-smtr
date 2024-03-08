{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'month' },
)}}

WITH idade_frota AS (
  SELECT
  data,
  EXTRACT(YEAR FROM LAST_DAY(data)) - CAST(ano_fabricacao AS INT64) AS idade
  FROM
    rj-smtr.veiculo.sppo_licenciamento
)
SELECT 
  MIN(LAST_DAY(data)) AS data,
  EXTRACT(YEAR FROM data) AS ano,
  EXTRACT(MONTH FROM data) AS mes,
  ROUND(AVG(idade),2) AS indicador_idade_media
FROM idade_frota
GROUP BY
  2,3


-- todos os veículos licenciados ativos no mês, no último dia do mês
