-- depends_on: {{ ref('aux_sppo_licenciamento_vistoria_atualizada') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    -- Tabela de licenciamento
    stu as (
        select 
            *
        from
            {{ ref("sppo_licenciamento_stu_staging") }} as t
        where
        {% if var("stu_data_versao") != "" %}
            data = date("{{ var('stu_data_versao') }}")
        -- Versão fixa do STU em 2024-03-25 para mar/Q1 devido à falha de atualização na fonte da dados (SIURB)
        {%- elif var("run_date") >= "2024-03-01" and var("run_date") < "2024-03-16" %}
            data = "2024-03-25"
        -- Versão fixa do STU em 2024-04-09 para mar/Q2 devido à falha de atualização na fonte da dados (SIURB)
        {%- elif var("run_date") >= "2024-03-16" %}
            data = "2024-04-09"
        {% else %}
            {% if execute %}
                {% set licenciamento_date = run_query("SELECT MIN(data) FROM " ~ ref("sppo_licenciamento_stu_staging") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 5 DAY)").columns[0].values()[0] %}
            {% endif %}

            data = DATE("{{ licenciamento_date }}")
        {% endif %}
            and tipo_veiculo not like "%ROD%"
    ),
    stu_rn AS (
        select
            * except (timestamp_captura),
            EXTRACT(YEAR FROM data_ultima_vistoria) AS ano_ultima_vistoria,
            ROW_NUMBER() OVER (PARTITION BY data, id_veiculo) rn
        from
            stu
    ),
    stu_ano_ultima_vistoria AS (
        -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
        {% if var("run_date") >= "2024-03-01" %}
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            CASE 
                WHEN c.ano_ultima_vistoria > s.ano_ultima_vistoria THEN c.ano_ultima_vistoria
                ELSE COALESCE(s.ano_ultima_vistoria, c.ano_ultima_vistoria)
            END AS ano_ultima_vistoria_atualizado,
        FROM
            stu_rn AS s
        LEFT JOIN
            (
                SELECT
                    id_veiculo, 
                    placa,
                    ano_ultima_vistoria
                FROM
                    {{ ref("aux_sppo_licenciamento_vistoria_atualizada") }}
            ) AS c
        USING
            (id_veiculo, placa)
        {% else %}
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            s.ano_ultima_vistoria AS ano_ultima_vistoria_atualizado,
        FROM
            stu_rn AS s
        {% endif %}
    )
select
  * except(rn),
from
  stu_ano_ultima_vistoria
where
  rn = 1

