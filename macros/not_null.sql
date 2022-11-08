{%test not_null(model, column_name, partition_column)%}
SELECT
    {{column_name}}
FROM
    {{model}}
WHERE
    {{column_name}} IS NULL
{% if partition_column != NULL %}
AND
    {{partition_column}} = (SELECT MAX({{partition_column}}) FROM {{model}})   
{% endif %}
{%endtest%}