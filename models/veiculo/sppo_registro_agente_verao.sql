{{ 
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    unique_key="id_registro",
    incremental_strategy="merge",
    merge_update_columns=["data", "datetime_registro", "id_registro", "id_veiculo", "servico", "link_foto", "validacao"],
  )
}}

WITH 
  registro AS (
    SELECT
      data,
      datetime_registro,
      SHA256(datetime_registro || "_" || email) AS id_registro,
      id_veiculo,
      servico,
      link_foto,
      validacao,
      datetime_captura,
      "{{ var("version") }}" AS versao
    FROM
      {{ ref("registro_agente_verao_tr_subtt") }}
    WHERE
      validacao = TRUE
  ),
  registro_rn AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_registro) AS rn
    FROM
      registro
  )
SELECT
  * EXCEPT(rn)
FROM
  registro_rn
WHERE
  rn = 1