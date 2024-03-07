{{config( 
    partition_by = { 'field' :'mes',
    'data_type' :'int',
    'granularity': 'month' },
)}}

WITH consorcio AS (
  SELECT
    id_consorcio,
    c.consorcio,
    (SELECT modo FROM rj-smtr.cadastro.modos WHERE id_modo='3' AND fonte='jae') AS modo
  FROM
    {{ref('consorcios')}} c
  WHERE
    c.id_consorcio = '221000050'
)
SELECT
  rdo.ano,
  rdo.mes,
  c.modo,
  SUM(rdo.qtd_grt_idoso + rdo.qtd_grt_especial +
      rdo.qtd_grt_estud_federal + rdo.qtd_grt_estud_estadual +
      rdo.qtd_grt_estud_municipal + rdo.qtd_grt_rodoviario +
      rdo.qtd_grt_passe_livre_universitario) AS indicador_gratuidade_mes
FROM
   {{source('br_rj_riodejaneiro_rdo','rdo40_tratado')}} AS rdo
LEFT JOIN 
  consorcio AS c ON rdo.termo = c.id_consorcio
WHERE 
  rdo.data BETWEEN "2015-01-01" AND "2023-06-30" AND c.id_consorcio IS NOT NULL
GROUP BY 
  rdo.ano, rdo.mes, c.modo
