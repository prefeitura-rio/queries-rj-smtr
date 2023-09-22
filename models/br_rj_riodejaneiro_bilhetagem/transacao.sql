SELECT 
    EXTRACT(DATE FROM data_transacao) AS data,
    data_transacao AS datetime_transacao,
    data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    id AS id_transacao,
    NULL AS id_veiculo,
    COALESCE(id_cliente, pan_hash) AS id_cliente,
    id_tipo_midia AS id_tipo_pagamento,
    tipo_transacao AS id_tipo_transacao,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    NULL as id_integracao_individual,
    g.ds_grupo AS modo,
    l.nr_linha AS servico,
    sentido,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    NULL AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao
FROM
    {{ ref("staging_transacao") }} AS t
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
