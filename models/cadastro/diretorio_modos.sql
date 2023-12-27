{{
  config(
    materialized="table",
    alias="modos"
  )
}}

{%
    set relacao_modo = [
        {"id_jae": "'1'", "id_stu": ""},

    ]
%}

WITH jae AS (
    SELECT
        cd_tipo_modal,
        ds_tipo_modal
    FROM
        {{ ref('staging_tipo_modal') }}
),
stu AS (
    SELECT
        id_modo,
        CASE
            WHEN id_modo = '4' THEN '1'
            WHEN id_modo = '8' THEN '1'
            WHEN id_modo = '2' THEN '2'
        END AS id_modo_jae,
        modo
    FROM
    (
        SELECT DISTINCT
            COALESCE(pj.id_modo, pf.id_modo) AS id_modo,
            COALESCE(pj.modo, pf.modo) AS modo
        FROM
            {{ ref("staging_operadora_empresa") }} pj
        FULL OUTER JOIN
            {{ ref("staging_operadora_pessoa_fisica") }} pf
        ON pf.id_modo = pj.id_modo
    )  
)
SELECT
    s.id_modo AS id_modo_stu,
    j.cd_tipo_modal AS id_modo_jae,
    CASE
        WHEN s.id_modo = '4' THEN 'Van (STPC)'
        WHEN s.id_modo = '8' THEN 'Van (STPL)'
        WHEN s.id_modo = '2' THEN 'Ônibus (SPPO)'
        ELSE COALESCE(s.modo, j.ds_tipo_modal)
    CASE
        WHEN s.id_modo = '8' THEN '8'
        WHEN s.id_modo = '4' THEN '9'
    END AS id_consorcio_jae,
    END AS modo
FROM
    jae j
FULL OUTER JOIN
    stu s
ON
    j.cd_tipo_modal = s.id_modo_jae
