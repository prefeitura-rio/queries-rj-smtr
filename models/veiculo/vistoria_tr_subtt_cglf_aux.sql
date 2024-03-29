{{
  config(
    materialized='ephemeral'
  )
}}

/* Modelo para aglutinar todas os dados com fonte TR/SUBTT/CGLF */

SELECT 
  id_veiculo,
  placa,
  MAX(ano_ultima_vistoria) AS ano_ultima_vistoria,
FROM
    (
        SELECT
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            {{ ref("vistoria_tr_subtt_cglf_2023") }}
        UNION ALL
        SELECT
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            {{ ref("vistoria_tr_subtt_cglf_2024") }}
        UNION ALL
        SELECT
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            {{ ref("vistoria_tr_subtt_cglf_pendentes_2024") }}
    )
GROUP BY
  1,
  2