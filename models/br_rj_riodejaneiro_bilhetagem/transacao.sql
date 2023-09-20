SELECT 
    EXTRACT(DATE FROM data_transacao) AS data,
    g.ds_grupo AS modo,
    data_transacao AS datetime_transacao,
    data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    id AS id_transacao,
    NULL AS id_veiculo,
    CASE
        WHEN id_cliente IS NULL THEN pan_hash
        ELSE id_cliente
    END AS id_cliente,
    id_tipo_midia AS id_tipo_pagamento,
    tipo_transacao AS id_tipo_transacao,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    NULL as id_integracao_individual,
    NULL AS servico,
    sentido,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    l.nr_linha AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao
FROM
    {{ ref("staging_transacao") }} AS t
LEFT JOIN
    {{ ref("staging_linha") }} AS l
LEFT JOIN
    {{ ref("staging_grupo_linha") }} AS gl
LEFT JOIN
    {{ ref("staging_grupo") }} AS g
USING (cd_linha)
