{{
  config( 
    partition_by = { 
    "field": "data",
    "data_type": "date",
    "granularity": "month"
    },
)}}

SELECT
  MIN(data) AS data,
  EXTRACT(YEAR FROM data) AS ano,
  EXTRACT(MONTH FROM data) AS mes,
  "Ônibus" AS modo,
  COUNT(DISTINCT id_veiculo) AS quantidade_veiculo_mes,
  CURRENT_DATE() AS data_ultima_atualizacao,
  '{{ var("version") }}' as versao
FROM
  {{ ref('viagem_completa') }}
  --rj-smtr.projeto_subsidio_sppo.viagem_completa
WHERE
{% if is_incremental() %}
  data BETWEEN DATE_TRUNC(DATE("{{ var("start_date") }}"), MONTH)
  AND LAST_DAY(DATE("{{ var("end_date") }}"), MONTH)
  AND data < DATE_TRUNC(CURRENT_DATE(), MONTH)
{% else %}
  data < DATE_TRUNC(CURRENT_DATE(), MONTH)
{% endif %}
GROUP BY
  2,
  3

