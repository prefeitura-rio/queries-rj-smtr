{% test contained(model, column_name, in, field) %}
select * 
from (
    SELECT DISTINCT
        {{ column_name }} contained,
        _contains
    FROM {{model}}
    LEFT JOIN (
        SELECT DISTINCT
            {{ field }} _contains
        FROM {{ in }}
    )
    ON {{column_name }} = _contains
)
where _contains is null
{% endtest %}