{% macro filter_veiculo_datetime(id_veiculo_amostra, datetime_partida_amostra, datetime_chegada_amostra) %}
  {% set conditions = [] %}
  {% for id_veiculo, datetime_partida, datetime_chegada in zip(id_veiculo_amostra, datetime_partida_amostra, datetime_chegada_amostra) %}
    {% do conditions.append("OR (SUBSTRING(id_veiculo, 2) = '" ~ id_veiculo ~ "' AND timestamp_gps BETWEEN '" ~ datetime_partida ~ "' AND '" ~ datetime_chegada ~ "')") %}
  {% endfor %}
  {{ return(conditions | join(' ')) }}
{% endmacro %}