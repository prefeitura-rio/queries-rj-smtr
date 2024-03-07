{{config( 
    partition_by = { 'field' :'mes',
    'data_type' :'date',
    'granularity': 'day' },
)}}

WITH idade_frota AS (
  SELECT
    LAST_DAY(data) AS ultimo_dia_mes,
    EXTRACT(YEAR FROM LAST_DAY(data)) - CAST(ano_fabricacao AS INT64) AS idade
  FROM
    {{ref('sppo_licenciamento')}}
)
SELECT 
  MIN(ultimo_dia_mes) AS data,
  EXTRACT(YEAR FROM ultimo_dia_mes) AS ano,
  EXTRACT(MONTH FROM ultimo_dia_mes) AS mes,
  ROUND(AVG(idade),2) AS indicador_idade_media
FROM idade_frota
GROUP BY
  2,3


-- todos os veículos licenciados ativos no mês, no último dia do mês
