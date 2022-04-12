select  
    data_versao,
    json_value(content, '$.IDFrotaDeterminada') IDFrotaDeterminada,
    json_value(content, '$.TipoOnibusID') TipoOnibusID,
    SAFE_CAST(json_value(content, '$.FrotaDeterminada') AS INT64) FrotaDeterminada,
    SAFE_CAST(json_value(content, '$.FrotaServico') AS INT64) FrotaServico,
    json_value(content, '$.dataInicioVigencia') dataInicioVigencia,
    json_value(content, '$.dataFimVigencia') dataFimVigencia,
    json_value(content, '$.legislacaoInicioVigencia') legislacaoInicioVigencia,
    json_value(content, '$.legislacaoFimVigencia') legislacaoFimVigencia,
    json_value(content, '$.route_id') route_id
FROM {{ ref('frota_determinada') }}
