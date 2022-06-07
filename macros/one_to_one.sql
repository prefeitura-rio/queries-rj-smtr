{% test one_to_one(model, column_name, partition_column, to_table) %}
{% if execute %}
{% set model_max_partition = run_query('SELECT MAX('~partition_column~') FROM '~model).columns[0].values()[0] %}
{% set to_table_max_partition = run_query('SELECT MAX('~partition_column~') FROM '~to_table).columns[0].values()[0] %}
{% endif %}
with t as (
SELECT
    m.{{column_name}} from_col,
    n.{{column_name}} to_col
FROM (
    SELECT
        {{column_name}}
    FROM {{model}}
    WHERE {{partition_column}} = "{{model_max_partition}}"
) m
LEFT JOIN (
    SELECT
        {{column_name}}
    FROM {{to_table}}
    WHERE {{partition_column}} = "{{to_table_max_partition}}"
) n
ON m.{{column_name}} = n.{{column_name}}
)
SELECT *
FROM (
    SELECT 
        from_col,
        count(to_col) ct
    FROM t
    GROUP BY from_col
) 
where ct != 1
{% endtest %}
