{{config( 
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
)}}

WITH operadoras AS (
  SELECT
    perm_autor
  FROM
    `rj-smtr.br_rj_riodejaneiro_stu.operadora_empresa`
  WHERE
    modo = "Ônibus"
    AND razao_social IN ('CONSORCIO OPERACIONAL BRT')
)

SELECT
  data,
  "BRT" AS modo,
  SUM(qtd_grt_idoso+qtd_grt_especial+
      qtd_grt_estud_federal+qtd_grt_estud_estadual+
      qtd_grt_estud_municipal+qtd_grt_rodoviario+
      qtd_grt_passe_livre_universitario) AS indicador_gratuidade_mes,

FROM
  (
  SELECT
    rdo.data,
    rdo.qtd_grt_idoso, 
    rdo.qtd_grt_especial,
    rdo.qtd_grt_estud_federal,
    rdo.qtd_grt_estud_estadual,
    rdo.qtd_grt_estud_municipal,
    rdo.qtd_grt_rodoviario,
    rdo.qtd_grt_passe_livre_universitario,
    ROW_NUMBER() OVER (PARTITION BY rdo.data ORDER BY rdo.data) AS row_num
  FROM
    `rj-smtr.br_rj_riodejaneiro_rdo.rdo40_tratado` AS rdo
  LEFT JOIN 
    operadoras AS op
  ON 
    rdo.termo = op.perm_autor
  WHERE 
    rdo.data >= "2015-01-01" AND rdo.data <="2023-06-30" AND op.perm_autor IS NOT NULL
) subquery
GROUP BY 
  data
-- razão social CONSORCIO OPERACIONAL BRT
