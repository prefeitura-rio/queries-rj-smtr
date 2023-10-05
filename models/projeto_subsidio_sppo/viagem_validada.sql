{{ 
config(
    materialized='incremental',
    partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
    },
    unique_key=['id_viagem'],
    incremental_strategy='insert_overwrite'
)
}}

WITH
  viagem_planejada AS (
  SELECT
    DISTINCT 
    DATA,
    servico,
    MAX(distancia_total_planejada) AS km_planejada,
    ROUND(MAX(distancia_total_planejada)/SUM(distancia_planejada), 2) AS viagens_planejadas
  FROM
    {{ ref("viagem_planejada") }}
  {% if is_incremental() %}
  WHERE
      data = date_sub(date("{{ var("run_date") }}"), interval 1 day)
  {% endif %}
  GROUP BY
    1,
    2)
-- Filtra viagens até a quantidade de viagens planejadas, observados os limites de tolerância da RESOLUÇÃO SMTR Nº 3645/2023
SELECT
  c.* EXCEPT(rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY DATA, servico_informado ORDER BY datetime_partida) AS rn
  FROM
    {{ ref("viagem_completa") }} ) AS c
LEFT JOIN
  viagem_planejada AS p
ON
  c.data = p.data
  AND c.servico_informado = p.servico
WHERE
  CASE
    WHEN c.tipo_dia = "Dia Útil" AND viagens_planejadas > 10 AND rn > viagens_planejadas*1.2 THEN FALSE
    WHEN c.tipo_dia = "Dia Útil" AND viagens_planejadas <= 10 AND rn > viagens_planejadas*2 THEN FALSE
  ELSE
  TRUE
END
  = TRUE