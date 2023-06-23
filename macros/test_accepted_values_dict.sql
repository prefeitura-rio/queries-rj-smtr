{% test accepted_values_dict(model, column_name, ref_model, table_name) %}

WITH
  values_dict AS (
  SELECT
    chave
  FROM
    {{ ref(ref_model) }}
  WHERE
    id_tabela = "{{ table_name }}"
    AND nome_coluna = "{{ column_name }}" ),
  all_values AS (
  SELECT
    {{ column_name }} AS value_field,
    COUNT(*) AS n_records
  FROM {{ model }}
  GROUP BY
    {{ column_name }} )
SELECT
  *
FROM
  all_values
WHERE
  value_field NOT IN (
  SELECT
    *
  FROM
    values_dict )

{% endtest %}