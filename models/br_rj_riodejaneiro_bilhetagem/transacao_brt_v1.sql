SELECT 
    NULL AS id_veiculo,
    CASE
        WHEN id_cliente IS NULL THEN pan_hash
        ELSE id_cliente
    END AS id_cliente,
    id AS id_transacao,
    tipo_transacao AS id_tipo_transacao,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    t.data,
    data_transacao AS datetime_transacao,
    is_abt AS indicador_cliente_registrado,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    NULL AS tipo_pagamento,
    sentido,
    NULL AS servico,
    l.nr_linha AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao
FROM
    {{ ref("transacao") }} AS t
LEFT JOIN
    {{ ref("linha") }} AS l
USING (cd_linha)
