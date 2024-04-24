{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Modelo de relacionamento entre run_date e data de licenciamento dos modelos:
    - `sppo_licenciamento_stu_staging.sql`
    - `sppo_licenciamento.sql`
    - `sppo_veiculo_dia.sql`
*/

SELECT
  "{{ var('run_date') }}" AS run_date,
  CASE
    {% if var("stu_data_versao") != "" %}
    WHEN "{{ var('stu_data_versao') }}" != "" THEN DATE("{{ var('stu_data_versao') }}")
    {% endif %}
    --- Versão fixa do STU em 2024-03-25 para mar/Q1 devido à falha de atualização na fonte da dados (SIURB)
    WHEN "{{ var('run_date') }}" >= "2024-03-02" AND "{{ var('run_date') }}" <= "2024-03-16" THEN DATE("2024-03-25")
    -- Versão fixa do STU em 2024-04-09 para mar/Q2 devido à falha de atualização na fonte da dados (SIURB)
    WHEN "{{ var('run_date') }}" >= "2024-03-17" AND "{{ var('run_date') }}" <= "2024-04-01" THEN DATE("2024-04-09")
    ELSE (
      SELECT MIN(data)
      FROM {{ ref("sppo_licenciamento_stu_staging") }}
      WHERE data >= DATE_ADD(DATE("{{ var('run_date') }}"), INTERVAL 5 DAY)
        -- Admite apenas versões do STU igual ou após 2024-04-09 a partir de abril/24 devido à falha de atualização na fonte da dados (SIURB)
        AND ("{{ var('run_date') }}" < "2024-04-02" OR data >= '2024-04-09')
    )
  END AS licenciamento_date