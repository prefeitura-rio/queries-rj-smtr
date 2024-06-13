{{
    config(
        materialized="table",
    )
}}

WITH matriz_melt AS (
  SELECT
    i.id,
    im.sequencia_integracao,
    im.id_tipo_modal,
    im.perc_rateio,
    i.dt_inicio_validade,
    i.dt_fim_validade
  FROM
    {{ ref("staging_percentual_rateio_integracao") }} i,
    -- Transforma colunas com os dados de cada modo da integração em linhas diferentes
    UNNEST(
      [
        {% for n in range(var('quantidade_integracoes_max')) %}
          STRUCT(
            {% for column in ['id_tipo_modal' , 'perc_rateio'] %}
              {{ column }}{{ '_integracao_t'~n if n > 0 else '_origem' }} AS {{ column }},
            {% endfor %}
            {{ n }} AS sequencia_integracao
          ){% if not loop.last %},{% endif %}
        {% endfor %}
      ]
    ) im
)
SELECT
  i.id AS id_matriz_integracao,
  i.sequencia_integracao,
  m.modo,
  i.perc_rateio AS percentual_rateio,
  i.dt_inicio_validade AS data_inicio_validade,
  i.dt_fim_validade AS data_fim_validade,
  '{{ var("version") }}' as versao
FROM
  matriz_melt i
LEFT JOIN 
    {{ source("cadastro", "modos") }} m
ON
  i.id_tipo_modal = m.id_modo AND m.fonte = "jae"
WHERE
  i.id_tipo_modal IS NOT NULL