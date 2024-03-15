{% set queries = [
    {"table": "passageiro_pagante", "indicator": "Passageiros pagantes por mês", "field": "quantidade_passageiro_pagante_mes"},
    {"table": "passageiro_gratuidade", "indicator": "Gratuidades por mês", "field": "quantidade_passageiro_gratuidade_mes"},
    {"table": "frota_operante", "indicator": "Frota operante por mês", "field": "quantidade_veiculo_mes"},
    {"table": "idade_media_frota_operante_onibus", "indicator": "Idade média da frota operante por mês", "field": "idade_media_veiculo_mes"}
] %}

{% for query in queries %}
SELECT 
    ano,
    mes,
    {% if query.mode is defined %}NULL{% else %}modo{% endif %} AS modo,
    "{{ query.indicator }}" AS nome_indicador,
    {{ query.field }} AS valor,
    data_ultima_atualizacao
FROM {{ ref(query.table) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}