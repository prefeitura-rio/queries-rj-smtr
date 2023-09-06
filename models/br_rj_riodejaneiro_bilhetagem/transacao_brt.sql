SELECT 
    t.data,
    NULL AS id_veiculo,
    CASE
        WHEN id_cliente IS NULL THEN pan_hash
        ELSE id_cliente
    END AS id_cliente,
    id AS id_transacao,
    tipo_transacao AS id_tipo_transacao,
    id_tipo_midia AS id_tipo_pagamento,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    data_transacao AS datetime_transacao,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    NULL AS servico,
    sentido,
    l.nr_linha AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao
FROM
    {{ ref("transacao") }} AS t
LEFT JOIN
    {{ ref("linha") }} AS l
USING (cd_linha)
