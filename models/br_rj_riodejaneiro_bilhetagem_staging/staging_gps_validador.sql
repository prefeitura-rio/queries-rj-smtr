{{
  config(
    alias='gps_validador',
  )
}}


SELECT
    data,
    hora,
    REPLACE(SAFE_CAST(id AS STRING), ".0", "") AS id,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
    SAFE_CAST(JSON_VALUE(content, '$.bytes_recebidos_app') AS FLOAT64) AS bytes_recebidos_app,
    SAFE_CAST(JSON_VALUE(content, '$.bytes_recebidos_geral') AS FLOAT64) AS bytes_recebidos_geral,
    SAFE_CAST(JSON_VALUE(content, '$.bytes_transmitidos_app') AS FLOAT64) AS bytes_transmitidos_app,
    SAFE_CAST(JSON_VALUE(content, '$.bytes_transmitidos_geral') AS FLOAT64) AS bytes_transmitidos_geral,
    SAFE_CAST(JSON_VALUE(content, '$.codigo_linha_veiculo') AS STRING) AS codigo_linha_veiculo,
    REPLACE(SAFE_CAST(JSON_VALUE(content, '$.codigo_operadora') AS STRING), ".0", "") AS codigo_operadora,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_tracking') AS STRING)), 'America/Sao_Paulo') AS data_tracking,
    SAFE_CAST(JSON_VALUE(content, '$.estado_equipamento') AS STRING) AS estado_equipamento,
    SAFE_CAST(JSON_VALUE(content, '$.fabricante_equipamento') AS STRING) AS fabricante_equipamento,
    SAFE_CAST(JSON_VALUE(content, '$.latitude_equipamento') AS FLOAT64) AS latitude_equipamento,
    SAFE_CAST(JSON_VALUE(content, '$.longitude_equipamento') AS FLOAT64) AS longitude_equipamento,
    SAFE_CAST(JSON_VALUE(content, '$.modelo_equipamento') AS STRING) AS modelo_equipamento,
    SAFE_CAST(JSON_VALUE(content, '$.numero_cartao_operador') AS STRING) AS numero_cartao_operador,
    SAFE_CAST(JSON_VALUE(content, '$.numero_chip_sam') AS STRING) AS numero_chip_sam,
    SAFE_CAST(JSON_VALUE(content, '$.numero_chip_telefonia') AS STRING) AS numero_chip_telefonia,
    SAFE_CAST(JSON_VALUE(content, '$.numero_serie_equipamento') AS STRING) AS numero_serie_equipamento,
    SAFE_CAST(JSON_VALUE(content, '$.prefixo_veiculo') AS FLOAT64) AS prefixo_veiculo,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_transacoes_enviadas') AS FLOAT64) AS qtd_transacoes_enviadas,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_transacoes_pendentes') AS FLOAT64) AS qtd_transacoes_pendentes,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_venda_botao') AS FLOAT64) AS qtd_venda_botao,
    SAFE_CAST(JSON_VALUE(content, '$.sentido_linha') AS STRING) AS sentido_linha,
    SAFE_CAST(JSON_VALUE(content, '$.tarifa_linha') AS FLOAT64) AS tarifa_linha,
    SAFE_CAST(JSON_VALUE(content, '$.versao_app') AS FLOAT64) AS versao_app,
    SAFE_CAST(JSON_VALUE(content, '$.temperatura') AS FLOAT64) AS temperatura
FROM
    {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'gps_validador') }}

