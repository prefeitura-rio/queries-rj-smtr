{% test contained(model, column_name, in, field, partition_column) %}
select * 
from (
    SELECT DISTINCT
        {{ column_name }} contained,
        _contains
    FROM (
        SELECT DISTINCT
            {{column_name}}
        FROM {{model}}
        {% if execute -%}
        {%- set query ="SELECT MAX(" ~ partition_column ~ ") FROM " ~ model -%}
        {%- set max_partition_date = run_query(query).columns[0].values()[0] -%}
        {{- log(query, info=True) -}}
        {{- log(max_partition_date, info=True) -}}
        {%- endif -%}
        WHERE {{partition_column}} = "{{max_partition_date}}"
    )
    LEFT JOIN (
        SELECT DISTINCT
            {{ field }} _contains
        FROM {{ in }}
        WHERE {{partition_column}} = "{{max_partition_date}}"
    )
    ON {{column_name }} = _contains
)
where _contains is null
{% endtest %}