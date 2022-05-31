{% test unique_by_date(model, column_name, date_column_name) %}
SELECT 
    *
FROM (
    SELECT
        {{ column_name }},
        {{ date_column_name }},
        ROW_NUMBER() over (partition by {{ column_name }}, {{ date_column_name }}) rn
    FROM
        {{ model }}
    WHERE
        {{ date_column_name }} = (select max({{ date_column_name }}) from {{ model }})
)

WHERE rn>1
{% endtest %}