{{
  config(
    alias="transacao"
  )
}}

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
            data >= "{{ var('run_date') }}"
    )
    WHERE
        rn = 1
)



SELECT
  EXTRACT(DATE FROM data_transacao) AS data,
  EXTRACT(HOUR FROM data_transacao) AS hora,
  COUNT(*) AS Qtde_Passageiros_hora
FROM
  transacao_deduplicada
WHERE
  data >= "2024-05-04"
  AND cd_operadora = "2359"
  AND cd_linha = "1126"
GROUP BY
  data,
  hora