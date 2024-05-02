
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
  id as id_transacao  
FROM
  transacao_deduplicada
WHERE
  cd_linha = "1033"
