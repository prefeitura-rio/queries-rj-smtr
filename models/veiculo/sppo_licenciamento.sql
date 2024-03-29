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
            {{ ref("sppo_licenciamento_stu") }} as t
        where
        {% if var("stu_data_versao") != "" %}
            data = date("{{ var('stu_data_versao') }}")
        {% else %}
            -- Versão fixa do STU em 2024-03-25 devido à falha de atualização na fonte da dados (SIURB)
            {%- if var("run_date") >= "2024-03-01" %}
                {% set licenciamento_date = "2024-03-25" %}
            {% else %}
                {% if execute %}
                    {% set licenciamento_date = run_query("SELECT MIN(data) FROM " ~ ref("sppo_licenciamento_stu") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 5 DAY)").columns[0].values()[0] %}
                {% endif %}
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
  * except(rn, data_inicio_vinculo), 
  CASE 
    WHEN ano_ultima_vistoria_atualizado >= CAST(EXTRACT(YEAR FROM DATE_SUB("{{ var('run_date') }}", INTERVAL {{ var('sppo_licenciamento_validade_vistoria_ano') }} YEAR)) AS INT64) THEN TRUE -- Última vistoria realizada dentro do período válido
    WHEN data_ultima_vistoria IS NULL AND DATE_DIFF(DATE("{{ var('run_date') }}"), data_inicio_vinculo, DAY) <=  {{ var('sppo_licenciamento_tolerancia_primeira_vistoria_dia') }} THEN TRUE -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
  ELSE FALSE
  END AS indicador_vistoria_valida,
from
  stu_ano_ultima_vistoria
where
  rn = 1

