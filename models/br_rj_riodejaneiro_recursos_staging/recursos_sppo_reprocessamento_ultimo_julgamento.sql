{{  config(
    materialized = 'incremental',
  )
}}
WITH exploded AS (
  SELECT 
    id_recurso,
    datetime_update AS data_julgamento, 
    SAFE_CAST(COALESCE(JSON_VALUE(items, '$.value'), JSON_VALUE(items, '$.items[0].customFieldItem')) AS STRING
    ) AS julgamento, 
    SAFE_CAST(JSON_EXTRACT(items, '$.customFieldId') AS STRING ) AS field_id 
  FROM 
    {{ ref('staging_recursos_sppo_reprocessamento') }}, 
    UNNEST(items) items
  {% if is_incremental() -%}
    WHERE
        DATE(data) BETWEEN DATE("{{var('date_range_start')}}") 
        AND DATE("{{var('date_range_end')}}")
  {%- endif %}
), 
pivotado AS (
  SELECT * EXCEPT(field_id)
  FROM 
    exploded 
  WHERE field_id = '111865'

), 
{% if is_incremental() %}
  julgamento AS (
    SELECT 
      p.id_recurso,
      p.julgamento,
      p.data_julgamento,
      t.julgamento AS ultimo_julgamento    
    FROM 
      pivotado p
    LEFT JOIN 
      {{ this }} AS t
    USING (id_recurso)
  )
{% else %}
  julgamento AS (
    SELECT 
      id_recurso, 
      julgamento, 
      data_julgamento,
      LAG(julgamento) OVER (PARTITION BY id_recurso ORDER BY data_julgamento) AS ultimo_julgamento
    FROM
      pivotado 
    WHERE 
      julgamento IS NOT NULL
  )
{% endif %}

SELECT 
  id_recurso, 
  data_julgamento, 
  julgamento
FROM 
(
  SELECT 
    ROW_NUMBER() OVER(PARTITION BY j.id_recurso ORDER BY j.data_julgamento DESC) AS rn,
    *
  FROM 
    julgamento j
  WHERE 
    j.julgamento != j.ultimo_julgamento OR j.ultimo_julgamento IS NULL
)
WHERE rn=1
