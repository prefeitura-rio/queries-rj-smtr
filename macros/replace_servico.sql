{% macro replace_servico(id_veiculo_amostra, datetime_partida_amostra, datetime_chegada_amostra, servico_amostra) %}
  {% for id_veiculo, datetime_partida, datetime_chegada, servico in zip(id_veiculo_amostra, datetime_partida_amostra, datetime_chegada_amostra, servico_amostra) %}
    WHEN r.linha != 'GARAGEM' 
    AND SUBSTRING(r.id_veiculo, 2) = '{{ id_veiculo }}'
    AND r.timestamp_gps BETWEEN '{{ datetime_partida }}' AND '{{ datetime_chegada }}'
    THEN '{{ servico }}'
  {% endfor %}
{% endmacro %}