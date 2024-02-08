{{
    config(
        materialized="view"
    )
}}

SELECT
    do.modo,
    g.data,
    g.hora,
    g.data_tracking AS datetime_gps,
    g.timestamp_captura AS datetime_captura,
    do.id_operadora,
    do.operadora,
    l.nr_linha AS servico,
    NULL AS id_veiculo,
    g.numero_serie_equipamento AS id_validador,
    g.id AS id_transmissao_gps,
    g.latitude_equipamento AS latitude,
    g.longitude_equipamento AS longitude,
    INITCAP(g.sentido_linha) AS sentido,
    g.estado_equipamento,
    g.temperatura
FROM
    {{ ref("staging_gps_validador") }} g
LEFT JOIN
    {{ ref("operadoras") }} AS do
ON
    g.codigo_operadora = do.id_operadora_jae
LEFT JOIN
    {{ ref("staging_linha") }} AS l
ON
    g.codigo_linha_veiculo = l.cd_linha