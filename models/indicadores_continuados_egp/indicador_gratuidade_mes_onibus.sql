{{config(
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' }
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
  SUM(qtd_grt_idoso + qtd_grt_especial +
      qtd_grt_estud_federal + qtd_grt_estud_estadual +
      qtd_grt_estud_municipal + qtd_grt_rodoviario +
      qtd_grt_passe_livre_universitario) AS indicador_gratuidade_mes
FROM (
  SELECT
    op.modo,
    rdo.data,
    rdo.qtd_grt_idoso, 
    rdo.qtd_grt_especial,
    rdo.qtd_grt_estud_federal,
    rdo.qtd_grt_estud_estadual,
    rdo.qtd_grt_estud_municipal,
    rdo.qtd_grt_rodoviario,
    rdo.qtd_grt_passe_livre_universitario
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
