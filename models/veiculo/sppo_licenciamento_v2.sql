{{
    config(
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge",
    )
}}

/*
    Validar
        - unique_key
            - id_veiculo = "A41062" (aparentemente foi baixado e depois o número foi reutilizado)
        - merge_update_columns
            - merge_update_columns=["data_ultima_vistoria", "versao", "datetime_ultima_atualizacao", "data_fim_vinculo"]
            - Atualizar sempre versao e datetime_ultima_atualizacao sem atualizar as demais informações?
        
*/

{%- if var("run_date") >= "2024-03-01" %}
    {%- if execute %}
        {% set licenciamento_max_date = run_query("SELECT SAFE_CAST(MAX(DATA) AS DATE) FROM " ~ ref('sppo_licenciamento_stu') ~ " WHERE DATA >= DATE_SUB('" ~ var('run_date') ~ "', INTERVAL 5 DAY)").columns[0].values()[0] %}
    {% endif -%}

    WITH
    raw AS (
        SELECT
            id_veiculo || "_" || placa AS id,
            *
        FROM
        (
            SELECT
                * EXCEPT(data),
                data
            FROM
                {{ ref("sppo_licenciamento_stu") }}
            {%- if is_incremental() %}
            WHERE
                DATA >= DATE("{{ var('run_date') }}")
            {% endif -%}
            {%- if var("run_date") >= "2024-03-01" and var("run_date") <= "2024-03-15" %}
            UNION ALL
            SELECT
                * EXCEPT(data),
                DATE_ADD(DATE("{{ licenciamento_max_date }}"), INTERVAL 1 DAY) AS data
            FROM
                {{ ref("aux_sppo_licenciamento_stu") }}
            WHERE
                DATA = "2024-03-24"
            {% endif -%}
        )
        WHERE
            tipo_veiculo NOT LIKE "%ROD%"
            AND tipo_veiculo NOT LIKE "%BRT%"
    ),
    treated_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY data DESC) rn
        FROM
            raw
    )
    SELECT
        id,
        id_veiculo,
        placa,
        permissao,
        modo,      
        data_inicio_vinculo,
        CASE
            WHEN DATA < DATE("{{ licenciamento_max_date }}") THEN DATA
        ELSE
            NULL
        END AS data_fim_vinculo,
        data_ultima_vistoria,
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
        quantidade_lotacao_pe,
        quantidade_lotacao_sentado,
        tipo_combustivel,
        tipo_veiculo,
        status,
        timestamp_captura,
        "{{ var('version') }}" AS versao,
        CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao,
    FROM
        treated_rn
    WHERE
        rn = 1
{%- else -%}
    SELECT
        *
    FROM
        {{ this }}
{% endif -%}