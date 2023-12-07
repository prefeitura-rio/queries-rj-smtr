{{
  config(
    materialized="table",
    alias='consorcios'
  )
}}

WITH stu AS (
  SELECT 
      perm_autor AS id_consorcio,
      cnpj,
      processo,
      data_registro,
      razao_social,
      CASE
        WHEN perm_autor = "221000050" THEN "1"
        WHEN perm_autor = "221000032" THEN "3"
        WHEN perm_autor = "221000023" THEN "4"
        WHEN perm_autor = "221000041" THEN "5"
        WHEN perm_autor = "221000014" THEN "6"
      END AS cd_consorcio_jae
  FROM
    {{ ref("staging_operadora_empresa") }} AS stu
)
SELECT
  COALESCE(s.id_consorcio, j.cd_consorcio) AS id_consorcio,
  s.razao_social,
  j.nm_consorcio AS consorcio,
  s.processo AS id_processo,
  s.data_registro AS data_processo,
  s.cnpj,
  s.id_consorcio AS id_consorcio_stu,
  j.cd_consorcio AS id_consorcio_jae,
FROM {{ ref("staging_consorcio") }} AS j
LEFT JOIN
  stu AS s
ON
  j.cd_consorcio = s.cd_consorcio_jae

  
