{{
  config(
    materialized="incremental",
    partition_by={
        "field": "data_inicio_periodo_vistoria",
        "data_type": "date",
        "granularity": "day"
    },
    incremental_strategy="insert_overwrite",
  )
}}

WITH stu AS (
    SELECT
      *
    FROM
      {{ ref("sppo_licenciamento_stu") }}
    WHERE
      {%- if is_incremental() %}
      DATA >= DATE("{{ var('run_date') }}")
      AND 
      {% endif %}
      tipo_veiculo NOT LIKE "%ROD%"
      AND tipo_veiculo NOT LIKE "%BRT%"
  ),
  sppo_licenciamento AS (
    SELECT
        id_veiculo,
        placa,
        data_ultima_vistoria,
        FALSE AS indicador_data_teorica
    FROM
        stu
    UNION ALL
    SELECT
        id_veiculo,
        placa,
        data_ultima_vistoria,
        TRUE AS indicador_data_teorica
    FROM
        {{ ref("sppo_licenciamento_cglf_aux") }}
        -- TODO: checar se é apenas SPPO (sem rodoviários e BRT) - Não afeta a tabela principal, apenas para fins de organização com o nome da tabela
  ),
  sppo_licenciamento_treated AS (
  SELECT
    DISTINCT id_veiculo || "_" || placa || "_" || data_inicio_vinculo AS id,
    id_veiculo,
    placa,
    data_inicio_vinculo,
    data_ultima_vistoria,
    indicador_data_teorica
  FROM 
    sppo_licenciamento
  LEFT JOIN
    (SELECT DISTINCT
      id_veiculo,
      placa,
      data_inicio_vinculo
    FROM
      stu) AS s
  USING
    (id_veiculo, placa)
  WHERE
    data_ultima_vistoria IS NOT NULL 
  )
SELECT
  id,
  id_veiculo,
  placa,
  data_inicio_vinculo,
  data_ultima_vistoria AS data_inicio_periodo_vistoria,
  DATE_SUB(LEAD(data_ultima_vistoria) OVER (PARTITION BY id ORDER BY data_ultima_vistoria), INTERVAL 1 DAY) AS data_fim_periodo_vistoria,
  indicador_data_teorica
FROM
  sppo_licenciamento_treated