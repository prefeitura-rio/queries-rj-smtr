{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key=["id_transacao"],
    incremental_strategy="insert_overwrite"
  )
}}

SELECT 
    EXTRACT(DATE FROM data_processamento) AS data,
    EXTRACT(HOUR FROM data_processamento) AS hora,
    data_transacao AS datetime_transacao,
    data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    g.ds_grupo AS modo,
    c.nm_consorcio AS consorcio,
    -- TODO: Automatizar busca pela permissao no banco
    CASE
      WHEN t.cd_operadora = "1" THEN "22.100005-0"
    END AS permissao,
    pj.nm_fantasia AS empresa,
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
    AND t.data_transacao >= g.datetime_inclusao
LEFT JOIN
    {{ ref("staging_operadora_transporte") }} AS o
ON
    t.cd_operadora = o.cd_operadora_transporte
LEFT JOIN
    {{ ref("staging_pessoa_juridica") }} AS pj
ON
    o.cd_cliente = pj.cd_cliente
{% if is_incremental() -%}
WHERE 
    DATE(t.timestamp_captura) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{%- endif %}