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
SUM(qtd_buc_1_perna+qtd_buc_2_perna_integracao+
      qtd_buc_supervia_1_perna+qtd_buc_supervia_2_perna_integracao+
      qtd_cartoes_perna_unica_e_demais+qtd_pagamentos_especie) AS indicador_passageiro_pagante_mes,
FROM
   {{ref('rdo40_tratado')}} AS rdo
LEFT JOIN 
  consorcio AS c ON rdo.termo = c.id_consorcio
WHERE 
  rdo.data BETWEEN "2015-01-01" AND "2023-06-30" AND c.id_consorcio IS NOT NULL
GROUP BY 
  rdo.ano, rdo.mes, c.modo
