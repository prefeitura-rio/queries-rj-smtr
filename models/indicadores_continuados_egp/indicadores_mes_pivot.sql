{% set table_name = ref("indicadores_mes") %}

{% if execute %}
    {% set result = run_query("SELECT DISTINCT nome_indicador, modo FROM " ~ table_name) %}
    {% set indicadores_modos = {} %}
    {% for row in result %}
        {% set indicador = row['nome_indicador'] %}
        {% set modo = row['modo'] %}
        {% if indicador in indicadores_modos %}
            {% do indicadores_modos[indicador].append(modo) %}
        {% else %}
            {% do indicadores_modos.update({indicador: [modo]}) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% set outer_loop_iteration = namespace(value=0) %}
{% set total_indicators = indicadores_modos|length %}

{% for indicador, modos in indicadores_modos.items() %}
    {% set outer_loop_iteration.value = outer_loop_iteration.value + 1 %}
    {% for modo in modos %}
    SELECT 
        "{{ indicador }}" AS indicador, 
        {% if modo %}"{{ modo }}"{% else %}NULL{% endif %} AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                {{ table_name }}
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE 
                nome_indicador = "{{ indicador }}"
                AND modo {% if modo %}= "{{ modo }}"{% else %}IS NULL{% endif %}
        ) PIVOT ( MAX(valor) FOR mes IN ( 
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro ) 
        )
    )
    {% if outer_loop_iteration.value != total_indicators %}UNION ALL{% endif %}
    {% endfor %}
{% endfor %}