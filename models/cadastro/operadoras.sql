{{
  config(
    materialized="table"
  )
}}

WITH operadora_jae AS (
  SELECT
    ot.cd_operadora_transporte,
    ot.cd_cliente,
    m.modo,
    ot.cd_tipo_modal,
    ot.ds_tipo_modal AS modo_jae,
    -- STU considera BRT como Ônibus
    CASE
      WHEN ot.cd_tipo_modal = '3' THEN 'Ônibus'
      ELSE m.modo
    END AS modo_join,
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
    cb.nr_conta
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
  JOIN 
    {{ source("cadastro", "modos") }} m
  ON
    ot.cd_tipo_modal = m.id_modo AND m.fonte = "jae"
),
stu_pessoa_juridica AS (
  SELECT
    perm_autor,
    cnpj AS documento,
    processo,
    id_modo,
    modo AS modo_stu,
    tipo_permissao,
    data_registro,
    razao_social AS nome_operadora,
    "CNPJ" AS tipo_documento
  FROM
    {{ ref("staging_operadora_empresa") }}
  WHERE perm_autor NOT IN ({{ var("ids_consorcios").keys()|reject("equalto", "'229000010'")|join(", ") }})
),
stu_pessoa_fisica AS (
  SELECT
    perm_autor,
    cpf AS documento,
    processo,
    id_modo,
    modo AS modo_stu,
    tipo_permissao,
    data_registro,
    nome AS nome_operadora,
    "CPF" AS tipo_documento
  FROM
    {{ ref("staging_operadora_pessoa_fisica") }}
),
stu AS (
  SELECT
    s.*,
    m.modo
  FROM (
    SELECT
      *
    FROM
      stu_pessoa_juridica

    UNION ALL

    SELECT
      *
    FROM
      stu_pessoa_fisica
  ) s
  JOIN 
    {{ source("cadastro", "modos") }} m
  ON
    s.id_modo = m.id_modo AND m.fonte = "stu"
),
cadastro AS (
  SELECT 
    COALESCE(s.perm_autor, j.cd_operadora_transporte) AS id_operadora,
    UPPER(REGEXP_REPLACE(NORMALIZE(COALESCE(s.nome_operadora, j.nm_cliente), NFD), r"\pM", '')) AS operadora_completo,
    s.tipo_permissao AS tipo_operadora,
    COALESCE(j.modo, s.modo) AS modo,
    s.modo_stu,
    j.modo_jae,
    s.processo AS id_processo,
    s.data_registro AS data_processo,
    COALESCE(s.documento, j.nr_documento) AS documento,
    COALESCE(s.tipo_documento, j.tipo_documento) AS tipo_documento,
    s.perm_autor AS id_operadora_stu,
    j.cd_operadora_transporte AS id_operadora_jae,
    SAFE_CAST(j.in_situacao_atividade AS BOOLEAN) AS indicador_operador_ativo_jae,
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
    AND s.modo = j.modo_join
)
SELECT
  id_operadora,
  modo,
  modo_stu,
  modo_jae,
  CASE
    WHEN tipo_documento = "CNPJ" THEN operadora_completo
    ELSE REGEXP_REPLACE(operadora_completo, '[^ ]', '*')
  END AS operadora,
  operadora_completo,
  tipo_operadora,
  tipo_documento,
  documento,
  codigo_banco,
  banco,
  agencia,
  tipo_conta,
  conta,
  id_operadora_stu,
  id_operadora_jae,
  id_processo,
  data_processo,
  indicador_operador_ativo_jae
FROM
  cadastro
WHERE
  modo NOT IN ("Escolar", "Táxi", "TEC", "Fretamento")