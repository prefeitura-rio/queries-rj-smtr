{{
config(
    materialized = 'view',
    )
}}

{% set dimensoes = {'VALIDITY':'validade','COMPLETENESS':'completude', 'ACCURACY':'acuracia','FRESHNESS':'atualizacao', 'UNIQUENESS':'exclusividade', 'CONSISTENCY':'consistencia'}%}

WITH 
last_jobs as (
    SELECT DISTINCT
        data_quality_job_id job_id,
        row_number() over (partition by data_particao, data_source.table_id order by job_start_time DESC) rn
        from {{ref('testes_qualidade')}}
),
{{var('table_id')}} as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        data_particao,
        hora_particao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        {% for key, value in dimensoes.items()%}
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.{{ key }}.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.{{ key }}.passed") = "false"
            THEN 0
        ELSE NULL 
        END as {{value}},
        {% endfor %}
    from {{ref('testes_qualidade')}} as dq
)
SELECT
    DISTINCT t.*
from {{var('table_id')}} t
join (SELECT * FROM last_jobs WHERE rn=1) l
ON l.job_id = t.job_id