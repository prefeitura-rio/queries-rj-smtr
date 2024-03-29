{{
  config(
    materialized='ephemeral'
  )
}}

/* Dados auxiliares de vistoria de ônibus levantados pela Coordenadoria Geral de Licenciamento e Fiscalização (TR/SUBTT/CGLF), para atualização da data de última vistoria informada no sistema (STU). */

SELECT
  data,
  id_veiculo,
  placa,
  MAX(ano_ultima_vistoria) AS ano_ultima_vistoria,
FROM
    (
        SELECT
            data,
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            {{ ref("vistoria_tr_subtt_cglf_2023") }}
        UNION ALL
        SELECT
            data,
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            {{ ref("vistoria_tr_subtt_cglf_2024") }}
        UNION ALL
        SELECT
            data,
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            {{ ref("vistoria_tr_subtt_cglf_pendentes_2024") }}
    )
GROUP BY
  1,
  2,
  3