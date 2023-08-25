{% macro test_date_range(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < '2022-06-01' OR {{ column_name }} > current_date

{% endmacro %}