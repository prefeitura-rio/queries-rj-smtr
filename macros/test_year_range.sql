{% macro test_year_range(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 2022 OR {{ column_name }} > extract(year from current_date)

{% endmacro %}