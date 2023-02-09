{{ 
config(
    alias='valor_multa_dia'
)
}}

WITH
  classificacao AS (
  SELECT
    data,
    faixa_km
  FROM
    `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
  CROSS JOIN
    UNNEST([STRUCT(0 AS start,
        40 AS finish), STRUCT(40 AS start,
        60 AS finish)]) AS faixa_km )
SELECT
  *,
  CASE
    WHEN faixa_km.start = 0 AND faixa_km.finish = 40 THEN 1126.55 -- ART. 5º-A, II - DEC RIO N. 51.889/2022 ALTERADO PELO DEC RIO N. 51940/2023
    WHEN faixa_km.start = 40 AND faixa_km.finish = 60 THEN 563.28 -- ART. 5º-A, I - DEC RIO N. 51.889/2022 ALTERADO PELO DEC RIO N. 51940/2023
  ELSE
  0
END
  AS valor_multa
FROM
  classificacao