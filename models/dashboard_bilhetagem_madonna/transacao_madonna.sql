
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
            data in ("2024-05-04", "2024-05-05")
    )
    WHERE
        rn = 1
)
SELECT
  EXTRACT(DATE FROM data_transacao) AS data,
  FORMAT_TIMESTAMP("%H:%M:%S", data_transacao) AS hora,
  id as id_transacao  
FROM
  transacao_deduplicada
WHERE
  cd_operadora = "2359"
  AND cd_linha = "1126"