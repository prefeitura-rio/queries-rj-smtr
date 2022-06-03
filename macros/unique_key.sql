{% test unique_key(model, column_name, partition_column, combined_keys) %}
SELECT 
    *
FROM (
    SELECT
        {{ column_name }},
        {{ partition_column }},
        ROW_NUMBER() over (partition by {{(column_name~","~combined_keys|join(',')) if combined_keys != "" else column_name}}) rn
    FROM
        {{ model }}
    WHERE
        {{ partition_column }} = (select max({{ partition_column }}) from {{ model }})
)

WHERE rn>1
{% endtest %}