{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data_particao",
      "data_type":"date",
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite"
  )
}}

{% set dimensoes = {'VALIDITY':'validade','COMPLETENESS':'completude', 'ACCURACY':'acuracia','FRESHNESS':'atualizacao', 'UNIQUENESS':'exclusividade', 'CONSISTENCY':'consistencia'}%}

SELECT 
    data_particao,
    hora_particao,
    dataset_id,
    table_id,
    CASE
        {% for key, value in dimensoes.items()%}
        WHEN {{value}} = 0
        THEN 'ruim'
        {% endfor %}
    ELSE 'bom'
    end as indice_qualidade
from {{ref('indicadores_view')}}
