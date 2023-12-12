-- depends_on: {{ ref('operadoras_contato') }}
{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key="id_transacao",
  )
}}
WITH transacao_aberta AS (
    SELECT
        data,
        hora,
        id,
        timestamp_captura,
        SAFE_CAST(JSON_VALUE(content, '$.assinatura') AS STRING) AS assinatura,
        SAFE_CAST(JSON_VALUE(content, '$.cd_aplicacao') AS STRING) AS cd_aplicacao,
        SAFE_CAST(JSON_VALUE(content, '$.cd_emissor') AS STRING) AS cd_emissor,
        SAFE_CAST(JSON_VALUE(content, '$.cd_linha') AS STRING) AS cd_linha,
        SAFE_CAST(JSON_VALUE(content, '$.cd_matriz_integracao') AS STRING) AS cd_matriz_integracao,
        SAFE_CAST(JSON_VALUE(content, '$.cd_operadora') AS STRING) AS cd_operadora,
        SAFE_CAST(JSON_VALUE(content, '$.cd_secao') AS STRING) AS cd_secao,
        SAFE_CAST(JSON_VALUE(content, '$.cd_status_transacao') AS STRING) AS cd_status_transacao,
        DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_processamento') AS STRING)), "America/Sao_Paulo") AS data_processamento,
        DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_transacao') AS STRING)), "America/Sao_Paulo") AS data_transacao,
        SAFE_CAST(JSON_VALUE(content, '$.id_cliente') AS STRING) AS id_cliente,
        SAFE_CAST(JSON_VALUE(content, '$.id_produto') AS STRING) AS id_produto,
        SAFE_CAST(JSON_VALUE(content, '$.id_servico') AS STRING) AS id_servico,
        SAFE_CAST(JSON_VALUE(content, '$.id_tipo_midia') AS STRING) AS id_tipo_midia,
        SAFE_CAST(JSON_VALUE(content, '$.is_abt') AS BOOL) AS is_abt,
        SAFE_CAST(JSON_VALUE(content, '$.latitude_trx') AS FLOAT64) AS latitude_trx,
        SAFE_CAST(JSON_VALUE(content, '$.longitude_trx') AS FLOAT64) AS longitude_trx,
        SAFE_CAST(JSON_VALUE(content, '$.nr_logico_midia_operador') AS STRING) AS nr_logico_midia_operador,
        SAFE_CAST(JSON_VALUE(content, '$.numero_serie_validador') AS STRING) AS numero_serie_validador,
        SAFE_CAST(JSON_VALUE(content, '$.pan_hash') AS STRING) AS pan_hash,
        SAFE_CAST(JSON_VALUE(content, '$.posicao_validador') AS STRING) AS posicao_validador,
        SAFE_CAST(JSON_VALUE(content, '$.sentido') AS STRING) AS sentido,
        SAFE_CAST(JSON_VALUE(content, '$.tipo_integracao') AS STRING) AS tipo_integracao,
        SAFE_CAST(JSON_VALUE(content, '$.tipo_transacao') AS STRING) AS tipo_transacao,
        SAFE_CAST(JSON_VALUE(content, '$.uid_origem') AS STRING) AS uid_origem,
        SAFE_CAST(JSON_VALUE(content, '$.valor_tarifa') AS FLOAT64) AS valor_tarifa,
        SAFE_CAST(JSON_VALUE(content, '$.valor_transacao') AS FLOAT64) AS valor_transacao,
        SAFE_CAST(JSON_VALUE(content, '$.veiculo_id') AS STRING) AS veiculo_id,
        SAFE_CAST(JSON_VALUE(content, '$.vl_saldo') AS FLOAT64) AS vl_saldo
    FROM
        {{ source("br_rj_riodejaneiro_bilhetagem_staging", "transacao") }}
    {% if is_incremental() -%}
    WHERE
        DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
        AND DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
    {%- endif %}
),
transacao_deduplicada AS (
    SELECT 
        * EXCEPT(rn)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            transacao_aberta
    )
    WHERE
        rn = 1
)
SELECT 
    EXTRACT(DATE FROM data_transacao) AS data,
    EXTRACT(HOUR FROM data_transacao) AS hora,
    data_transacao AS datetime_transacao,
    data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    g.ds_grupo AS modo,
    dc.id_consorcio AS id_consorcio,
    do.id_operadora AS id_operadora,
    l.nr_linha AS servico,
    sentido,
    NULL AS id_veiculo,
    COALESCE(id_cliente, pan_hash) AS id_cliente,
    id AS id_transacao,
    id_tipo_midia AS id_tipo_pagamento,
    tipo_transacao AS id_tipo_transacao,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    NULL AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao,
    '{{ var("version") }}' as versao
FROM
    transacao_deduplicada AS t
LEFT JOIN
    {{ ref("staging_linha") }} AS l
ON
    t.cd_linha = l.cd_linha 
    AND t.data_transacao >= l.datetime_inclusao
LEFT JOIN
    {{ ref("staging_grupo_linha") }} AS gl
ON 
    t.cd_linha = gl.cd_linha 
    AND t.data_transacao >= gl.datetime_inicio_validade
    AND (t.data_transacao <= gl.datetime_fim_validade OR gl.datetime_fim_validade IS NULL)
LEFT JOIN
    {{ ref("staging_grupo") }} AS g
ON 
    gl.cd_grupo = g.cd_grupo
    AND t.data_transacao >= g.datetime_inclusao
LEFT JOIN
    {{ ref("staging_linha_consorcio") }} AS lc
ON 
    t.cd_linha = lc.cd_linha
    AND t.data_transacao >= lc.datetime_inicio_validade
    AND (t.data_transacao <= lc.datetime_fim_validade OR lc.datetime_fim_validade IS NULL)
LEFT JOIN
    {{ ref("staging_consorcio") }} AS c
ON 
    lc.cd_consorcio = c.cd_consorcio
LEFT JOIN
    {{ ref("staging_operadora_transporte") }} AS o
ON
    t.cd_operadora = o.cd_operadora_transporte
LEFT JOIN
    {{ ref("diretorio_operadoras") }} AS do
ON
    t.cd_operadora = do.id_operadora_jae
LEFT JOIN
    {{ ref("diretorio_consorcios") }} AS dc
ON
    lc.cd_consorcio = dc.id_consorcio_jae
