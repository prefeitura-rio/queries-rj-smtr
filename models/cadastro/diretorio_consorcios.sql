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
        {% for id_stu, id_jae in var("ids_consorcios").items() %}
          WHEN perm_autor = {{ id_stu }} THEN {{ id_jae }}
        {% endfor %}
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
  j.cd_consorcio AS id_consorcio_jae
FROM {{ ref("staging_consorcio") }} AS j
LEFT JOIN
  stu AS s
ON
  j.cd_consorcio = s.cd_consorcio_jae

  
