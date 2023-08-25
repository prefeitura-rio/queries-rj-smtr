{% macro test_zero_or_negative(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} > 0

{% endmacro %}