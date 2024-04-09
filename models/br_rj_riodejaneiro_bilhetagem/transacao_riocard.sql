{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
  )
}}

WITH transacao_deduplicada AS (
  SELECT
    * EXCEPT(rn)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
    FROM
      {{ ref('staging_transacao_riocard') }}
    {% if is_incremental() %}
      WHERE
        DATE(data) = DATE_SUB(DATE('{{ var("run_date") }}'), INTERVAL 1 DAY)
    {% endif %}
  )
  WHERE
    rn = 1
)
SELECT
  EXTRACT(DATE FROM data_transacao) AS data,
  EXTRACT(HOUR FROM data_transacao) AS hora,
  data_transacao AS datetime_transacao,
  data_processamento AS datetime_processamento,
  timestamp_captura AS datetime_captura,
  cd_consorcio AS id_consorcio_jae,
  cd_operadora AS id_operadora_jae,
  cd_linha AS id_servico_jae,
  numero_serie_validador AS id_validador,
  sentido,
  COALESCE(id_cliente, pan_hash) AS id_cliente,
  id AS id_transacao,
  latitude_trx AS latitude,
  longitude_trx AS longitude,
  valor_transacao,
  '{{ var("version") }}' as versao
FROM
  transacao_deduplicada