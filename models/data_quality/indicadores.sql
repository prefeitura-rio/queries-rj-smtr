{{
config(
    materialized = 'incremental',
    partition_by={
            "field":"data_execucao",
            "data_type": "date",
            "granularity":"day"
    },
    unique_key='job_id',
    )
}}

WITH 
{% for table in var('target') %}
{{table}} as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        {% for dimension in ['VALIDITY','COMPLETENESS', 'ACCURACY', 'VOLUME', 'FRESHNESS', 'UNIQUENESS', 'CONSISTENCY']%}
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.{{ dimension }}.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.{{ dimension }}.passed") = "false"
            THEN 0
        ELSE NULL 
        END as {{dimension|lower}},
        {% endfor %}
    from rj-smtr.bq_logs.{{table}}_data_quality as dq
    {% if is_incremental() %}
    WHERE dq.job_start_time = (SELECT MAX(job_start_time) from rj-smtr.bq_logs.{{table}}_data_quality)
    {% endif %}

){% if not loop.last %},{% endif %}
{% endfor %} 

{% for table in var('target')  %}
SELECT
    DISTINCT *
from {{table}}
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}