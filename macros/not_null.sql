{%test not_null(model, column_name, partition_column)%}
SELECT
    {{column_name}}
FROM
    {{model}}
WHERE
    {{partition_column}} = (SELECT MAX({{partition_column}}) FROM {{model}})
AND
    {{column_name}} is null
{%endtest%}