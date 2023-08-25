{% macro test_is_integer(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} != CAST({{ column_name }} AS INT)

{% endmacro %}
