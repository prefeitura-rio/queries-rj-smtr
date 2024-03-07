{{config(
    partition_by = { 'field' :'mes',
    'data_type' :'int',
    'granularity': 'month' }
)}}

      -- trocar o dataset, puxar as colunas do cadastro, 
      -- juntar no cadastro pela tabela do consorcio e o termo na tabela de RDO
      -- colocar tudo como mês
      -- ano, mês, modo, indicador, valor -> tabela final
WITH consorcio AS (
  SELECT
    id_consorcio,
    c.consorcio,
    (SELECT modo FROM rj-smtr.cadastro.modos WHERE id_modo='2' AND fonte='stu') AS modo
  FROM
    {{ref('consorcios')}} c
  WHERE
    c.id_consorcio IN ('221000041', '221000014', '221000032', '221000023')
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
   {{ref('rdo40_tratado')}} AS rdo
LEFT JOIN 
  consorcio AS c ON rdo.termo = c.id_consorcio
WHERE 
  rdo.data >= "2015-01-01" AND c.id_consorcio IS NOT NULL
GROUP BY 
  rdo.ano, rdo.mes, c.modo
