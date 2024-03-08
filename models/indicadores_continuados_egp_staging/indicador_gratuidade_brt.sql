{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'month' },
)}}

WITH consorcio AS (
    SELECT
    id_consorcio,
    modo
  FROM
    `rj-smtr.cadastro.consorcios` c
  LEFT JOIN
    `rj-smtr.cadastro.operadoras` o
  ON
    c.id_consorcio_jae = o.id_operadora_jae
  WHERE
    consorcio="MobiRio"
)
SELECT
  DATE_TRUNC(data, MONTH) AS data,
  rdo.ano,
  rdo.mes,
  c.modo,
  SUM(rdo.qtd_grt_idoso + rdo.qtd_grt_especial +
      rdo.qtd_grt_estud_federal + rdo.qtd_grt_estud_estadual +
      rdo.qtd_grt_estud_municipal + rdo.qtd_grt_rodoviario +
      rdo.qtd_grt_passe_livre_universitario) AS indicador_gratuidade_mes
FROM
   --{{source('br_rj_riodejaneiro_rdo','rdo40_tratado')}} AS rdo
   `rj-smtr.br_rj_riodejaneiro_rdo.rdo40_tratado` AS rdo
LEFT JOIN 
  consorcio AS c ON rdo.termo = c.id_consorcio
WHERE 
  rdo.data BETWEEN "2015-01-01" AND "2023-06-30" AND c.id_consorcio IS NOT NULL
GROUP BY 
 data, rdo.ano, rdo.mes, c.modo
