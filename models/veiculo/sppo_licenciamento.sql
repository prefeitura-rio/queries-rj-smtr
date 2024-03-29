-- depends_on: {{ ref('vistoria_tr_subtt_cglf_aux') }}
{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data", 
            "data_type": "date", 
            "granularity": "day"
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% do log(var('run_date')) %}
-- TODO: este código não está funcionando. É necessário corrigir set run_date
{% set run_date = run_query("SELECT DATE_ADD(DATE('" ~ var('run_date') ~ "'), INTERVAL " ~ var('licenciamento_limite_maximo_dias') ~ " DAY)").columns[0].values()[0] %} 
{% do run_query("SELECT DATE_ADD(DATE('" ~ var('run_date') ~ "'), INTERVAL " ~ var('licenciamento_limite_maximo_dias') ~ " DAY) FROM " ~ ref("licenciamento_iplanrio_stu")).print_table() %}
{% set run_date = "2024-02-01" %}
{% do log(run_date) %}

-- TODO: alterar o parâmetro run_date para a data corrente na pipeline
{%- if execute %}
    {%- if var("stu_data_versao") != "" %}
        {% set licenciamento_date = var('stu_data_versao') %}
    -- Parâmetro temporário em razão de falha na captura dos dados do STU
    {%- elif run_date >= "2024-03-01" and run_date <= "2024-03-31" %}
        {% set licenciamento_date = var('sppo_licenciamento_data_versao_mar_24') %}
    {%- else -%}
        {% set licenciamento_date = run_query("SELECT MAX(data) FROM " ~ ref("licenciamento_iplanrio_stu") ~ " WHERE data <= DATE('" ~ run_date ~ "')").columns[0].values()[0] %}
    {% endif -%}
{% endif -%}

WITH
    stu AS (
        SELECT
            *
        FROM
        (
            SELECT
                * EXCEPT(data),
                "{{ run_date }}" AS data,
                EXTRACT(YEAR FROM data_ultima_vistoria) AS ano_ultima_vistoria,
                data AS data_versao_stu,
                ROW_NUMBER() OVER (PARTITION BY id_veiculo) AS rn
            FROM
                {{ ref("licenciamento_iplanrio_stu") }}
            WHERE
                data = DATE("{{ licenciamento_date }}")
                AND tipo_veiculo NOT LIKE "%ROD%"
                AND tipo_veiculo NOT LIKE "%BRT%"
        )
        WHERE
            rn = 1
    ),
    stu_ano_ultima_vistoria AS (
        -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
        {% if run_date >= "2024-03-01" %}
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            CASE 
                WHEN c.ano_ultima_vistoria > s.ano_ultima_vistoria THEN c.ano_ultima_vistoria
                ELSE COALESCE(s.ano_ultima_vistoria, c.ano_ultima_vistoria)
            END AS ano_ultima_vistoria_atualizado,
        FROM
            stu AS s
        LEFT JOIN
            (
                SELECT
                    id_veiculo, 
                    placa,
                    ano_ultima_vistoria
                FROM
                    {{ ref("vistoria_tr_subtt_cglf_aux") }}
                WHERE
                    data = "{{ var('vistoria_tr_subtt_cglf_data_versao_mar_24') }}"
            ) AS c
        USING
            (id_veiculo, placa)
        {% else %}
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            s.ano_ultima_vistoria AS ano_ultima_vistoria_atualizado,
        FROM
            stu AS s
        {% endif %}
    )
SELECT
    data,
    data_versao_stu,
    id_veiculo,
    placa,
    data_inicio_vinculo,
    data_ultima_vistoria,
    ano_ultima_vistoria_atualizado,
    ano_fabricacao,
    permissao,
    modo,      
    carroceria,
    id_carroceria,
    nome_chassi,
    id_chassi,
    id_fabricante_chassi,
    id_interno_carroceria,
    id_planta,
    indicador_ar_condicionado,
    indicador_elevador,
    indicador_usb,
    indicador_wifi,
    CASE 
        WHEN ano_ultima_vistoria_atualizado >= CAST(EXTRACT(YEAR FROM DATE_SUB("{{ run_date }}", INTERVAL {{ var('prazo_maximo_vistoria_anos') }} YEAR)) AS INT64) THEN TRUE -- Última vistoria realizada dentro do período válido
        WHEN data_ultima_vistoria IS NULL AND DATE_DIFF(DATE("{{ run_date }}"), data_inicio_vinculo, DAY) <=  {{ var('licenciamento_tolerancia_primeira_vistoria_dias') }} THEN TRUE -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
    ELSE FALSE
    END AS indicador_vistoria_valida,
    quantidade_lotacao_pe,
    quantidade_lotacao_sentado,
    tipo_combustivel,
    tipo_veiculo,
    status,
    timestamp_captura,
    "{{ var('version') }}" AS versao,
    CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao,
FROM
  stu_ano_ultima_vistoria