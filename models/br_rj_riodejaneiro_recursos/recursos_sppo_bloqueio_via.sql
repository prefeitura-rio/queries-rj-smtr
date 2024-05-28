{{ config(
  materialized = 'incremental',
  partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
      unique_key = 'id_recurso',
) }}

WITH exploded AS (
  SELECT
    id_recurso,
    datetime_recurso,
    datetime_captura,
    datetime_update, 
    SAFE_CAST(COALESCE(JSON_VALUE(items, '$.value'), JSON_VALUE(items, '$.items[0].customFieldItem')) AS STRING
    ) AS value, 
    SAFE_CAST(JSON_EXTRACT(items, '$.customFieldId') AS STRING ) AS field_id 
  FROM 
    {{ ref('staging_recursos_sppo_bloqueio_via') }}, 
    UNNEST(items) items
  {% if is_incremental() -%}
    WHERE
      DATE(data) BETWEEN DATE("{{var('date_range_start')}}") 
        AND DATE("{{var('date_range_end')}}")
  {%- endif %}
), 
pivotado AS (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY id_recurso ORDER BY datetime_captura DESC) AS rn, 
  FROM 
    exploded PIVOT(
      ANY_VALUE(value) FOR field_id IN (
        '111870', '111871', '111872',  
        '111901', '111865', '111867', '111868', 
        '111869', '111866', '111904', '125615', 
        '111900', '111874'
      )
    )
), 
tratado AS (
  SELECT
    id_recurso, 
    datetime_captura, 
    datetime_recurso,
    datetime_update,
    SAFE_CAST(p.111865 AS STRING) AS julgamento, 
    SAFE_CAST(p.111870 AS STRING) AS consorcio,
    CASE
      WHEN SAFE_CAST(p.111872 AS STRING) = "SR - Regular" THEN SAFE_CAST(p.111871 AS STRING)
      ELSE CONCAT(REPLACE(SPLIT(SAFE_CAST(p.111872 AS STRING), "-")[OFFSET(0)], " ", ""), SAFE_CAST(p.111871 AS STRING))
    END AS servico, 
     CASE
        WHEN SAFE_CAST(p.111901 AS STRING) = "Ida" THEN "I"
        WHEN SAFE_CAST(p.111901 AS STRING) = "Volta" THEN "V"
        WHEN SAFE_CAST(p.111901 AS STRING) = "Circular" THEN "C"
    END
      AS sentido,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',SAFE_CAST(p.111867 AS STRING), 'America/Sao_Paulo') AS data_viagem, 
    SAFE_CAST(p.111874 AS STRING) AS numero_relatorio_cimu,
    COALESCE(SAFE_CAST(p.111904 AS STRING), SAFE_CAST(p.111900 AS STRING)) AS motivo_julgamento, 
    SAFE_CAST(p.125615 AS STRING) AS observacao,
   
  FROM 
    pivotado p
  WHERE rn=1
)

SELECT
      t.id_recurso,
      DATE(datetime_recurso) AS data,
      t.datetime_captura,
      t.datetime_recurso,
      t.datetime_update,
      t.consorcio,
      t.servico,
      t.sentido,
      DATE(EXTRACT(date FROM TIMESTAMP(data_viagem))) AS data_viagem,     
      t.numero_relatorio_cimu,
      t.julgamento,
      t.motivo_julgamento,
      t.observacao AS observacao_julgamento,
      j.data_julgamento
  
FROM
      tratado t
      
LEFT JOIN 

    {{ ref('recursos_sppo_bloqueio_via_ultimo_julgamento') }} AS j
    
  ON t.id_recurso = j.id_recurso
