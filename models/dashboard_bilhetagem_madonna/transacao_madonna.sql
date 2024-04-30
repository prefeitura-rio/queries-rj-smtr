
WITH transacao_deduplicada AS (
    SELECT 
        * EXCEPT(rn)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            {{ ref('staging_transacao') }}
        where
            DATE(data) BETWEEN DATE("{{ var('run_date') }}") and DATE_ADD(DATE("{{ var('run_date') }}"), INTERVAL 1 DAY)
    )
    WHERE
        rn = 1
)



SELECT
  EXTRACT(DATE FROM data_transacao) AS data,
  EXTRACT(HOUR FROM data_transacao) AS hora,
  COUNT(*) AS quantidade_transacoes
FROM
  transacao_deduplicada
WHERE
  cd_operadora = "2359"
  AND cd_linha = "1126"
GROUP BY
  data,
  hora