{{
  config(
    materialized="table"
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
  j.nm_consorcio AS consorcio,
  s.cnpj,
  s.razao_social,
  s.id_consorcio AS id_consorcio_stu,
  j.cd_consorcio AS id_consorcio_jae,
  s.processo AS id_processo,
FROM {{ ref("staging_consorcio") }} AS j
LEFT JOIN
  stu AS s
ON
  j.cd_consorcio = s.cd_consorcio_jae

  
