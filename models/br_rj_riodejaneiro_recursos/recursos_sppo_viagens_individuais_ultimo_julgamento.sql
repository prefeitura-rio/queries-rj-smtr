{{  config(
    materialized = 'table',
  )
}}
WITH expandida AS(
  {{ 
    abrir_tabela_recursos(
     date_range_start=var('date_range_start'), 
     date_range_end=var('date_range_end'), 
     is_incremental=is_incremental()
    ) 
  }}
),
julgamento AS (
  SELECT 
    id_recurso, 
    julgamento, 
    motivo_julgamento, 
    datetime_update,
    LAG(julgamento) OVER (PARTITION BY id_recurso ORDER BY datetime_update) AS ultimo_julgamento
  FROM
    {{ ref('recursos_sppo_viagens_individuais_expandida') }}
  WHERE 
    julgamento IS NOT NULL
)
SELECT 
  * EXCEPT(rn)
FROM 
(
  SELECT 
    ROW_NUMBER() OVER(PARTITION BY j.id_recurso ORDER BY j.datetime_update DESC) AS rn,
    j.id_recurso, 
    j.julgamento, 
    j.motivo_julgamento, 
    j.datetime_update AS data_julgamento
  FROM 
    julgamento j
  WHERE 
    j.julgamento != j.ultimo_julgamento OR j.ultimo_julgamento IS NULL
)
WHERE rn = 1


