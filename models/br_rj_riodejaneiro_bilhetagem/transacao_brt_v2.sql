SELECT 
    t.data,
    id AS id_transacao,
    NULL AS id_veiculo,
    NULL AS servico,
    sentido,
    CASE
        WHEN id_cliente IS NULL THEN pan_hash
        ELSE id_cliente
    END AS id_cliente,
    is_abt AS indicador_cliente_registrado,
    tipo_transacao AS id_tipo_transacao,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    data_transacao AS datetime_transacao,
    latitude_trx AS latitude,
    latitude_trx AS longitude,
    l.nr_linha AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao
FROM
    {{ ref("transacao") }} AS t
LEFT JOIN
    {{ ref("linha") }} AS l
USING (cd_linha)
