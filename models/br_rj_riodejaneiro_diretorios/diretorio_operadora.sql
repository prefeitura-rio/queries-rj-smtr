{{
  config(
    materialized="table",
    alias='operadoras'
  )
}}

WITH operadora_jae AS (
  SELECT
    ot.cd_operadora_transporte,
    ot.cd_cliente,
    ot.ds_tipo_modal,
    ot.in_situacao_atividade,
    CASE
      WHEN c.in_tipo_pessoa_fisica_juridica = 'F' THEN 'CPF'
      WHEN c.in_tipo_pessoa_fisica_juridica = 'J' THEN 'CNPJ'
    END AS tipo_documento,
    c.nr_documento,
    c.nm_cliente,
    cb.cd_agencia,
    cb.cd_tipo_conta,
    cb.nm_banco,
    cb.nr_banco,
    cb.nr_conta,
  FROM
    {{ ref("staging_operadora_transporte") }} AS ot
  JOIN
    {{ ref("staging_cliente") }} AS c
  ON
    ot.cd_cliente = c.cd_cliente
  LEFT JOIN
    {{ ref("staging_conta_bancaria") }} AS cb
  ON
    ot.cd_cliente = cb.cd_cliente
),
stu_pessoa_juridica AS (
  SELECT
    perm_autor,
    cnpj AS documento,
    processo,
    modo,
    tipo_permissao,
    data_registro,
    razao_social AS nome_operadora,
    "CNPJ" AS tipo_documento
  FROM
    {{ ref("staging_operadora_empresa") }}
  WHERE perm_autor NOT IN ({{ var('ids_consorcios')|join(', ') }})
),
stu_pessoa_fisica AS (
  SELECT
    perm_autor,
    cpf AS documento,
    processo,
    modo,
    tipo_permissao,
    data_registro,
    nome AS nome_operadora,
    "CPF" AS tipo_documento
  FROM
    {{ ref("staging_operadora_pessoa_fisica") }}
),
stu AS (
  SELECT
    *
  FROM
    stu_pessoa_juridica

  UNION ALL

  SELECT
    *
  FROM
    stu_pessoa_fisica
)
SELECT 
  COALESCE(s.perm_autor, j.cd_operadora_transporte) AS id_operadora,
  s.perm_autor AS id_operadora_stu,
  j.cd_operadora_transporte AS id_operadora_transporte_jae,
  COALESCE(s.documento, j.nr_documento) AS documento,
  COALESCE(s.tipo_documento, j.tipo_documento) AS tipo_documento,
  s.processo AS processo_stu,
  s.data_registro,
  UPPER(REGEXP_REPLACE(NORMALIZE(COALESCE(s.nome_operadora, j.nm_cliente), NFD), r"\pM", '')) AS operadora,
  COALESCE(s.modo, j.ds_tipo_modal) AS modo,
  s.tipo_permissao,
  j.in_situacao_atividade AS situacao_atividade,
  j.cd_agencia AS agencia,
  j.cd_tipo_conta AS tipo_conta,
  j.nm_banco AS banco,
  LPAD(j.nr_banco, 3, '0') AS codigo_banco,
  j.nr_conta AS conta
FROM
  stu AS s
FULL OUTER JOIN
  operadora_jae AS j
ON
  s.documento = j.nr_documento
  AND s.modo = j.ds_tipo_modal
