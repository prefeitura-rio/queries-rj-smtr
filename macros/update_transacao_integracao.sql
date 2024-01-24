{% macro update_transacao_integracao(incremental) %}

    UPDATE {{ ref("transacao") }} t
    SET t.indicador_integracao = True
    FROM {{ this }} i
    WHERE
    {% if incremental %}
        t.data BETWEEN DATE_SUB(DATE("{{ var('date_range_start') }}"), INTERVAL 2 DAY) AND DATE("{{ var('date_range_end') }}") AND
    {% endif %}
    t.id_transacao = i.id_transacao

{% endmacro %}