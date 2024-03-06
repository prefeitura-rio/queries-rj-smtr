{{config( 
    partition_by = { 'field' :'data_materializacao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['data_materializacao'],
)}}

SELECT
  ano,
  mes,
  "ônibus" AS modo,
  SUM(qtd_grt_idoso+qtd_grt_especial+
      qtd_grt_estud_federal+qtd_grt_estud_estadual+
      qtd_grt_estud_municipal+qtd_grt_rodoviario+
      qtd_grt_passe_livre_universitario) AS indicador_gratuidade_mes,
  DATE('{{ var("data_materializacao_egp") }}') AS data_materializacao
FROM
  `rj-smtr.br_rj_riodejaneiro_rdo.rdo40_tratado`

WHERE data >= "2015-01-01" AND termo IN ("221000023", "221000032", "221000014", "221000041")

GROUP BY  ano, mes

