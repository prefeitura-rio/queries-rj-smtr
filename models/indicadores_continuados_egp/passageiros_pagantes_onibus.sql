{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
)}}

WITH operadoras AS (
  SELECT
    perm_autor,
    modo
  FROM
    `rj-smtr.br_rj_riodejaneiro_stu.operadora_empresa`
  WHERE
    modo = "Ônibus"
    AND razao_social IN ( 'CONSORCIO TRANSCARIOCA DE TRANSPORTES',
      'CONSORCIO SANTA CRUZ DE TRANSPORTES',
      'CONSORCIO INTERSUL DE TRANSPORTES',
      'CONSORCIO INTERNORTE DE TRANSPORTES')
)
SELECT
  data,
  subquery.modo,
  SUM(qtd_buc_1_perna+qtd_buc_2_perna_integracao+
      qtd_buc_supervia_1_perna+qtd_buc_supervia_2_perna_integracao+
      qtd_cartoes_perna_unica_e_demais+qtd_pagamentos_especie) AS indicador_passageiro_pagante_mes,
FROM
  (
  SELECT
    op.modo,
    rdo.data,
    rdo.qtd_buc_1_perna, 
    rdo.qtd_buc_2_perna_integracao,
    rdo.qtd_buc_supervia_1_perna,
    rdo.qtd_buc_supervia_2_perna_integracao,
    rdo.qtd_cartoes_perna_unica_e_demais,
    rdo.qtd_pagamentos_especie,
    ROW_NUMBER() OVER (PARTITION BY rdo.data ORDER BY rdo.data) AS row_num
  FROM
    `rj-smtr.br_rj_riodejaneiro_rdo.rdo40_tratado` AS rdo
  LEFT JOIN 
    operadoras AS op
  ON 
    rdo.termo = op.perm_autor
  WHERE 
    rdo.data >= "2015-01-01" AND op.perm_autor IS NOT NULL
) subquery
GROUP BY 
  data, subquery.modo
