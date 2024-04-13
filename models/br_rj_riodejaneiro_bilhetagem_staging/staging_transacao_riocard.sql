{{
    config(
        alias='transacao_riocard',
    )
}}

SELECT
    data,
    hora,
    id,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
    SAFE_CAST(JSON_VALUE(content, '$.assinatura') AS STRING) AS assinatura,
    SAFE_CAST(JSON_VALUE(content, '$.cd_aplicacao') AS STRING) AS cd_aplicacao,
    SAFE_CAST(JSON_VALUE(content, '$.cd_emissor') AS STRING) AS cd_emissor,
    SAFE_CAST(JSON_VALUE(content, '$.cd_consorcio') AS STRING) AS cd_consorcio,
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
    {{ source("br_rj_riodejaneiro_bilhetagem_staging", "transacao_riocard") }}