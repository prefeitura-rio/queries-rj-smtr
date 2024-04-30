{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data", 
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

WITH sequencias_validas AS (
  SELECT
    id_matriz_integracao,
    STRING_AGG(modo, ', ' ORDER BY sequencia_integracao) AS modos
  FROM
    {{ ref("matriz_integracao") }}
  GROUP BY
    id_matriz_integracao
),
integracao_agg AS (
  SELECT
    DATE(datetime_processamento_integracao) AS data,
    id_integracao,
    STRING_AGG(modo, ', ' ORDER BY sequencia_integracao) AS modos,
    MAX(datetime_transacao) AS datetime_primeira_transacao,
    MIN(datetime_transacao) AS datetime_ultima_transacao,
    MIN(intervalo_integracao) AS menor_intervalo
  FROM
    {{ ref("integracao") }}
  {% if is_incremental() %}
    WHERE 
      data BETWEEN DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY) AND DATE_ADD(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
      AND datetime_processamento_integracao = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
  {% endif %}
  GROUP BY
    1,
    2
),
indicadores AS (
  SELECT
    data,
    id_integracao,
    modos,
    modos NOT IN (SELECT DISTINCT modos FROM sequencias_validas) AS indicador_fora_matriz,
    CASE
      WHEN TIMESTAMP_DIFF(datetime_ultima_transacao, datetime_primeira_transacao, MINUTE) > 180 THEN "Integração maior que 3 horas"
      WHEN menor_intervalo < 5 THEN "Tempo entre pernas menor que 5 minutos"
    END AS tipo_tempo_invalido
  FROM
    integracao_agg
)
SELECT
  *,
  '{{ var("version") }}' as versao
FROM
  indicadores
WHERE
  indicador_fora_matriz = TRUE
  OR tipo_tempo_invalido IS NOT NULL