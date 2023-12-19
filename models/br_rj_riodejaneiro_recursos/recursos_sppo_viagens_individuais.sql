{{ config(
  materialized = 'incremental',
  partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
      unique_key = 'id_recurso',
      alias = 'recursos_sppo_viagens_individuais',
) }}

WITH captura AS( 
SELECT * 
FROM {{ ref('recursos_sppo_viagens_individuais_view') }} 
),
julgamento AS(
  SELECT * 
  FROM {{ ref('recursos_sppo_viagens_individuais_ultimo_julgamento') }}
),

JOIN captura 
ON captura.id_recurso = julgamento.id_recurso

