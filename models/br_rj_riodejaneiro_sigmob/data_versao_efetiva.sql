{{
    config(
        materialized='incremental',
        unique_key='data',
        partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
        }
    )
}}

with
agency as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_agency")}}") THEN DATE("{{var("data_inclusao_agency")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("agency") }}
    )
ON data = DATE(data_versao)
),
calendar as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_calendar")}}") THEN DATE("{{var("data_inclusao_calendar")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("calendar") }}
    )
ON data = DATE(data_versao)
),
frota_determinada as (
SELECT 
    data,
    DATE(data_versao) as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_frota_determinada")}}") THEN DATE("{{var("data_inclusao_frota_determinada")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("frota_determinada") }}
    )
ON DATE(data) = DATE(data_versao)
),
holidays as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_holidays")}}") THEN DATE("{{var("data_inclusao_holidays")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("holidays") }}
    )
ON data = DATE(data_versao)
),
linhas as (
    SELECT 
    data,
    DATE(data_versao) as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_linhas")}}") THEN DATE("{{var("data_inclusao_linhas")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("linhas") }}
    )
ON data = DATE(data_versao)
),
routes as (
SELECT 
    data,
    DATE(data_versao) as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_routes")}}") THEN DATE("{{var("data_inclusao_routes")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("routes") }}
    )
ON data = data_versao
),
shapes as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_shapes")}}") THEN DATE("{{var("data_inclusao_shapes")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("shapes_geom") }}
    )
ON data = DATE(data_versao)
),
stop_details as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_stop_details")}}") THEN DATE("{{var("data_inclusao_stop_details")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("stop_details") }}
    )
ON data = DATE(data_versao)
),
stop_times as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_stop_times")}}") THEN DATE("{{var("data_inclusao_stop_times")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("stop_times") }}
    )
ON data = DATE(data_versao)
),
stops as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_stops")}}") THEN DATE("{{var("data_inclusao_stops")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("stops") }}
    )
ON data = DATE(data_versao)
),
trips as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{var("data_inclusao_trips")}}") THEN DATE("{{var("data_inclusao_trips")}}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE())) data
LEFT JOIN (
    SELECT DISTINCT data_versao
    FROM {{ ref("trips") }}
    )
ON data = DATE(data_versao)
),
joined as (
    select
    DATE_ADD(s.data, INTERVAL 1 DAY) data,
    a.data_versao_efetiva as data_versao_efetiva_agency,
    c.data_versao_efetiva as data_versao_efetiva_calendar,
    f.data_versao_efetiva as data_versao_efetiva_frota_determinada,
    h.data_versao_efetiva as data_versao_efetiva_holidays,
    l.data_versao_efetiva as data_versao_efetiva_linhas,
    r.data_versao_efetiva as data_versao_efetiva_routes,
    s.data_versao_efetiva as data_versao_efetiva_shapes,
    sd.data_versao_efetiva as data_versao_efetiva_stop_details,
    st.data_versao_efetiva as data_versao_efetiva_stop_times,
    sp.data_versao_efetiva as data_versao_efetiva_stops,
    t.data_versao_efetiva as data_versao_efetiva_trips
    from agency a 
    join shapes s
    on s.data = a.data
    join calendar c
    on a.data = c.data
    join frota_determinada f
    on a.data = f.data
    join holidays h
    on a.data = h.data
    join linhas l
    on a.data = l.data
    join routes r
    on a.data = r.data
    join stops sp
    on a.data = sp.data
    join stop_details sd
    on a.data = sd.data
    join stop_times st
    on a.data = st.data
    join trips t
    on a.data = t.data
)
select * 
from joined 
{% if is_incremental() %}
    {%set max_date_partition = run_query("select max(DATE(data)) from " ~ this).columns[0].values()[0]%}
    where DATE(data) > ("{{max_date_partition}}")
{% endif %}