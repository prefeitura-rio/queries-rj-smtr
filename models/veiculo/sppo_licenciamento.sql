-- depends_on: {{ ref('sppo_licenciamento_stu') }}

{{
    config(
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge",
        merge_update_columns = [
            "permissao",
            "modo",
            "data_fim_vinculo",
            "data_ultima_vistoria",
            "id_planta",
            "indicador_ar_condicionado",
            "indicador_elevador",
            "indicador_usb",
            "indicador_wifi",
            "quantidade_lotacao_pe",
            "quantidade_lotacao_sentado",
            "tipo_combustivel",
            "tipo_veiculo",
            "status",
            "timestamp_ultima_captura",
            "versao",
            "datetime_ultima_atualizacao"
        ]
    )
}}

{%- if execute %}
    {% set licenciamento_max_date = run_query("SELECT SAFE_CAST(MAX(DATA) AS DATE) FROM " ~ ref('sppo_licenciamento_stu') ~ " WHERE DATA >= DATE_SUB('" ~ var('run_date') ~ "', INTERVAL 5 DAY)").columns[0].values()[0] %}
{% endif -%}

WITH
raw AS (
    SELECT
        id_veiculo || "_" || placa || "_" || data_inicio_vinculo AS id,
        *
    FROM
        (
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
        )
),
treated_rn AS (
    SELECT
        *,
        MIN(timestamp_captura) OVER (PARTITION BY id ORDER BY timestamp_captura) AS timestamp_primeira_captura,
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
        WHEN data < DATE("{{ licenciamento_max_date }}") AND data != "2024-03-24" THEN data 
        /* Em 2024-03-24 foi realizada subida de extração manual realizada pela Satiê (IplanRio)
        Há veículos que não se encontram nas capturas seguintes
        TODO: ajustar após correção de captura via FTP */
    ELSE
        NULL
    END AS data_fim_vinculo,
    v.data_ultima_vistoria,
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
    timestamp_primeira_captura,
    timestamp_captura AS timestamp_ultima_captura,
    "{{ var('version') }}" AS versao,
    CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao,
FROM
    treated_rn AS t
LEFT JOIN
    (
        SELECT
            DISTINCT id,
            MAX(data_inicio_periodo_vistoria) OVER (PARTITION BY id ORDER BY data_inicio_periodo_vistoria DESC) AS data_ultima_vistoria
        FROM
            {{ ref("sppo_licenciamento_vistoria_historico") }}
    ) AS v
USING
    (id)
WHERE
    rn = 1