{{
config(
    materialized = 'incremental',
    partition_by={
            "field":"data_particao",
            "data_type": "date",
            "granularity":"day"
        }
    )
}}

SELECT 
    *,
    DATE('{{var('data_particao')}}') data_particao,
    COALESCE({{var('hora_particao')}}, NULL) hora_particao
FROM `rj-smtr.bq_logs.{{var('table_id')}}_data_quality` dq
{% if is_incremental()%}
WHERE dq.job_start_time = (SELECT MAX(job_start_time) FROM `rj-smtr.bq_logs.{{var('table_id')}}_data_quality`)
{% endif %}
