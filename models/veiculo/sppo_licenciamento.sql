{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

WITH
    stu AS (
        SELECT
            * EXCEPT(data),
            DATE("{{ var("run_date") }}") AS data,
            EXTRACT(YEAR FROM data_ultima_vistoria) AS ano_ultima_vistoria
        FROM
            {{ ref("licenciamento_iplanrio_stu") }}
        WHERE
            {%- if var("stu_data_versao") != "" %}
                data = DATE("{{ var('stu_data_versao') }}")
            {%- else -%}
                {%- if execute %}
                    -- Parâmetro temporário em razão de falha na captura dos dados do STU
                    {%- if var("run_date") >= "2024-03-01" and var("run_date") <= "2024-03-31" %}
                        {% set licenciamento_date = "2024-03-25" %}
                    {%- else -%}
                        {% set licenciamento_date = run_query("SELECT MIN(data) FROM " ~ ref("licenciamento_iplanrio_stu") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 5 DAY)").columns[0].values()[0] %}
                    {% endif -%}
                {% endif -%}
                data = DATE("{{ licenciamento_date }}")
            {% endif -%}
                AND tipo_veiculo NOT LIKE "%ROD%"
                AND tipo_veiculo NOT LIKE "%BRT%"
    ),
    stu_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id_veiculo) AS rn
        FROM
            stu 
    ),
    stu_ano_ultima_vistoria AS (
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
            {% if var("run_date") >= "2024-03-01" and var("run_date") <= "2024-03-31" %}
            CASE 
                WHEN c.ano_ultima_vistoria > s.ano_ultima_vistoria THEN c.ano_ultima_vistoria
                ELSE COALESCE(s.ano_ultima_vistoria, c.ano_ultima_vistoria)
            END AS ano_ultima_vistoria_atualizado,
            {% else %}
            s.ano_ultima_vistoria AS ano_ultima_vistoria_atualizado,
            {% endif -%}
        FROM
            stu_rn AS s
        {% if var("run_date") >= "2024-03-01" and var("run_date") <= "2024-03-31" %}
        LEFT JOIN
            (
                SELECT
                    id_veiculo, 
                    placa,
                    ano_ultima_vistoria
                FROM
                    {{ ref("vistoria_tr_subtt_cglf_aux") }}
                WHERE
                    data = "2024-03-28"
            ) AS c
        USING
            (id_veiculo, placa)
        {% endif -%}
        WHERE
            rn = 1
    )
SELECT
    data,
    id_veiculo,
    placa,
    data_inicio_vinculo,
    data_ultima_vistoria,
    ano_ultima_vistoria_atualizado,
    permissao,
    modo,      
    ano_fabricacao,
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
        WHEN ano_ultima_vistoria_atualizado >= CAST(EXTRACT(YEAR FROM DATE_SUB("{{ var("run_date") }}", INTERVAL 1 YEAR)) AS INT64) THEN TRUE -- Última vistoria realizada dentro do período válido
        WHEN data_ultima_vistoria IS NULL AND DATE_DIFF(DATE("{{ var('run_date') }}"), data_inicio_vinculo, DAY) <= 10 THEN TRUE -- Tolerância de 10 dias para vistoria inicial
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