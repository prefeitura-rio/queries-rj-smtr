{% test unique_relation(model, column_name, partition_column, relation_field) %}
SELECT *
FROM (
    SELECT
        {{ column_name }},
        {{ partition_column}},
        COUNT({{ relation_field }}) ct
    FROM {{ model }}
    WHERE 
        {{ partition_column }} = (select max({{ partition_column }}) from {{ model }})
    GROUP BY 1, 2
)
WHERE ct != 1
{% endtest %}