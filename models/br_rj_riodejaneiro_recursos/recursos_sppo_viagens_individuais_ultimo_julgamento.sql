{{  config(
    materialized = 'incremental',
    unique_key = 'id_recurso',
    cluster_by = ['datetime_update'],  
    incremental_strategy = 'merge',
    incremental_predicates = [
      "recursos_sppo_viagens_individuais_view.datetime_update > dateadd(day, -1 current_date)"
    ]
  )
}}

WITH julgamento AS (
SELECT id_recurso, julgamento, motivo_julgamento, datetime_update,
LAG(julgamento) OVER (PARTITION BY id_recurso ORDER BY datetime_update) AS ultimo_julgamento
FROM
  {{ ref('recursos_sppo_viagens_individuais_view') }}
WHERE
  datetime_update > dateadd(day, -1, current_date)
),
captura_view AS (
  SELECT * 
  FROM {{ ref('recursos_sppo_viagens_individuais_view') }}
WHERE julgamento IS NOT NULL
)
MERGE INTO captura
USING julgamento
ON captura_view.id_recurso = julgamento.id_recurso
WHEN MATCHED AND captura.julgamento != julgamento.julgamento THEN
  UPDATE SET
    captura_view.julgamento = julgamento.julgamento,
    captura_view.motivo_julgamento = julgamento.motivo_julgamento,
    captura_view.datetime_update = julgamento.ultimo_julgamento

