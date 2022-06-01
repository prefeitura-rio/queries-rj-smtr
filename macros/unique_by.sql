{% test unique_by(model, column_name, partition_column, unique_keys) %}
SELECT 
    *
FROM (
    SELECT
        {{ column_name }},
        {{ partition_column }},
        ROW_NUMBER() over (partition by {{column_name}}, {{unique_keys|join(',')}}) rn
    FROM
        {{ model }}
    WHERE
        {{ partition_column }} = (select max({{ partition_column }}) from {{ model }})
)

WHERE rn>1
{% endtest %}