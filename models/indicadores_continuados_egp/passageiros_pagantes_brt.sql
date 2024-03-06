{{config( 
    partition_by = { 'field' :'data_materializacao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['data_materializacao'],
)}}


SELECT
  ano,
  mes,
  "brt" AS modo,
  SUM(qtd_buc_1_perna+qtd_buc_2_perna_integracao+
      qtd_buc_supervia_1_perna+qtd_buc_supervia_2_perna_integracao+
      qtd_cartoes_perna_unica_e_demais+qtd_pagamentos_especie) AS indicador_passageiro_pagante_mes,
  DATE('{{ var("data_materializacao_egp") }}') AS data_materializacao
FROM
  `rj-smtr.br_rj_riodejaneiro_rdo.rdo40_tratado`

WHERE data >= "2015-01-01" AND data <="2023-06-30" AND termo IN ("221000050")

GROUP BY  ano, mes
