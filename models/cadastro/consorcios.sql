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
  WHERE
    perm_autor IN ({{ var("ids_consorcios").keys()|join(", ") }})
)
SELECT
  COALESCE(s.id_consorcio, j.cd_consorcio) AS id_consorcio,
  CASE
    WHEN s.id_consorcio = '221000050' THEN "Consórcio BRT"
    ELSE j.nm_consorcio 
  END AS consorcio,
  s.cnpj,
  s.razao_social,
  s.id_consorcio AS id_consorcio_stu,
  j.cd_consorcio AS id_consorcio_jae,
  s.processo AS id_processo,
FROM {{ ref("staging_consorcio") }} AS j
FULL OUTER JOIN
  stu AS s
ON
  j.cd_consorcio = s.cd_consorcio_jae

  
