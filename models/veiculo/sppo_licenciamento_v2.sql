{{
    config(
        materialized="incremental",
        unique_key="id_veiculo",
        incremental_strategy="merge",
    )
}}

{%- if var("run_date") >= "2024-03-01" %}
    {% if execute %}
        {% set licenciamento_max_date = run_query("SELECT SAFE_CAST(MAX(DATA) AS DATE) FROM " ~ ref('sppo_licenciamento_stu') ~ " WHERE DATA >= DATE_SUB('" ~ var('run_date') ~ "', INTERVAL 5 DAY)").columns[0].values()[0] %}
    {% endif %}

    WITH
    raw AS (
        SELECT
            *,
            "{{ var('version') }}" AS versao
        FROM
            {{ ref("sppo_licenciamento_stu") }}
        WHERE
            DATA >= DATE("{{ var('run_date') }}")
    ),
    treated AS (
        SELECT
            DATA,
            modo,
            id_veiculo,
            SAFE_CAST(PARSE_DATETIME("%d/%m/%Y", data_inicio_vinculo) AS DATE) AS data_inicio_vinculo,
            ano_fabricacao,
            carroceria,
            CASE
                WHEN data_ultima_vistoria = "" THEN NULL
            ELSE
                SAFE_CAST(PARSE_DATETIME("%d/%m/%Y", data_ultima_vistoria) AS DATE)
            END AS data_ultima_vistoria,
            id_carroceria,
            id_chassi,
            id_fabricante_chassi,
            id_interno_carroceria,
            id_planta,
            indicador_ar_condicionado,
            indicador_elevador,
            indicador_usb,
            indicador_wifi,
            nome_chassi,
            permissao,
            placa,
            quantidade_lotacao_pe,
            quantidade_lotacao_sentado,
            tipo_combustivel,
            tipo_veiculo,
            status,
            timestamp_captura,
            versao,
            ROW_NUMBER() OVER (PARTITION BY id_veiculo ORDER BY DATA DESC) rn
        FROM
            raw
    )
    SELECT
        modo,
        id_veiculo,
        data_inicio_vinculo,
        CASE
            WHEN DATA != DATE("{{ licenciamento_max_date }}") THEN DATA
        ELSE
            NULL
        END AS data_fim_vinculo,
        ano_fabricacao,
        carroceria,
        data_ultima_vistoria,
        id_carroceria,
        id_chassi,
        id_fabricante_chassi,
        id_interno_carroceria,
        id_planta,
        indicador_ar_condicionado,
        indicador_elevador,
        indicador_usb,
        indicador_wifi,
        nome_chassi,
        permissao,
        placa,
        quantidade_lotacao_pe,
        quantidade_lotacao_sentado,
        tipo_combustivel,
        tipo_veiculo,
        status,
        timestamp_captura,
        versao,
    FROM
        treated
    WHERE
        rn = 1
{%- else -%}
    SELECT
        *
    FROM
        {{ this }}
{% endif -%}