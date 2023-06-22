{% macro get_where_subquery(relation) -%}
    {# This macro returns a subquery that filters the relation based on the where clause #}
    {# It also replaces the __day_month_year_partition__ and __date_partition__ macros #}
    {# with the correct where clause #}

    {% set where = config.get('where', '') %}

    {# Partição por dia, mês e ano #}

    {% if "__day_month_year_partition__" in where %}

        {% if var("date_range_start") == "None" %}

        {% set date_range_start = run_query("SELECT gr FROM (SELECT IF(MAX(data) > DATE('" ~ var("date_range_end") ~ "'), DATE('" ~ var("date_range_end") ~ "'), MAX(data)) AS gr FROM " ~ this ~ ")").columns[0].values()[0] %}

        {% else %}

        {% set date_range_start = var("date_range_start") %}

        {% endif %}

        {% set where_cond = "dia <= EXTRACT(DAY FROM DATE('"~ var("date_range_end") ~"'))  AND mes <= EXTRACT(MONTH FROM DATE('"~ var("date_range_end") ~"')) AND ano <= EXTRACT(YEAR FROM DATE('"~ var("date_range_end") ~"')) AND dia >= EXTRACT(DAY FROM DATE('"~ date_range_start ~"')) AND mes >= EXTRACT(MONTH FROM DATE('"~ date_range_start ~"')) AND ano >= EXTRACT(YEAR FROM DATE('"~ date_range_start ~"'))" %}

        {% set where = where | replace("__day_month_year_partition__", where_cond) %}
    {% endif %}
    
    {# Partição por data #}

    {% if "__date_partition__" in where %}

        {% if var("date_range_start") == "None" %}

        {% set date_range_start = run_query("SELECT gr FROM (SELECT IF(MAX(data) > DATE('" ~ var("date_range_end") ~ "'), DATE('" ~ var("date_range_end") ~ "'), MAX(data)) AS gr FROM " ~ this ~ ")").columns[0].values()[0] %}

        {% else %}

        {% set date_range_start = var("date_range_start") %}

        {% endif %}

        {% set where_cond = "data BETWEEN DATE('"~ date_range_start ~"') AND DATE('"~ var("date_range_end") ~"')" %}

        {% set where = where | replace("__date_partition__", where_cond) %}
    {% endif %}
    {% if where %}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}