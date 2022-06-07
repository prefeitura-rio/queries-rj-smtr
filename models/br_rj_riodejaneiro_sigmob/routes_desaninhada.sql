with desaninhada as (
    select  
    data_versao,
    json_value(content, '$.agency_id') agency_id,
    json_value(content, '$.route_short_name') route_short_name,
    json_value(content, '$.route_long_name') route_long_name,
    json_value(content, '$.route_desc') route_desc,
    json_value(content, '$.route_type') route_type,
    json_value(content, '$.route_url') route_url,
    json_value(content, '$.route_color') route_color,
    json_value(content, '$.route_text_color') route_text_color,
    json_value(content, '$.ATIVA') ATIVA,
    json_value(content, '$.ATUALIZADO') ATUALIZADO,
    json_value(content, '$.Descricao') Descricao,
    json_value(content, '$.idModalSmtr') idModalSmtr,
    json_value(content, '$.linha_id') linha_id,
    json_value(content, '$.brs') brs,
    json_value(content, '$.IDTipoServico') IDTipoServico,
    json_value(content, '$.IDVariacaoServico') IDVariacaoServico,
    json_value(content, '$.origem') origem,
    json_value(content, '$.destino') destino,
    json_value(content, '$.ClassificacaoEspacial') ClassificacaoEspacial,
    json_value(content, '$.ClassificacaoHierarquica') ClassificacaoHierarquica,
    json_value(content, '$.InicioVigencia') InicioVigencia,
    json_value(content, '$.LegisInicioVigencia') LegisInicioVigencia,
    json_value(content, '$.fimVigencia') fimVigencia,
    json_value(content, '$.LegisfimVigencia') LegisfimVigencia,
    json_value(content, '$.FlagVigente') FlagVigente,
    json_value(content, '$.FrotaDeterminada') FrotaDeterminada,
    json_value(content, '$.LegisFrota') LegisFrota,
    json_value(content, '$.FrotaServico') FrotaServico,
    json_value(content, '$.FrotaOperante') FrotaOperante,
    json_value(content, '$.Observacoes') Observacoes,
    json_value(content, '$.IDParadaOrigem') IDParadaOrigem,
    json_value(content, '$.SiglaServico') SiglaServico,
    json_value(content, '$.IDParadaDestino') IDParadaDestino,
    json_value(content, '$.route_id') route_id,
    json_value(content, '$.id') id,
    json_value(content, '$.agency_name') agency_name,
    json_value(content, '$.Via') Via,
    json_value(content, '$.Vista') Vista,
    json_value(content, '$.Complemento') Complemento,
    json_value(content,'$.OLD_routes_id') old_route_id
from {{ ref('routes') }}
),
ultimas_versoes as (
    select
        *
    from desaninhada
    where DATE(fimVigencia) >= DATE(data_versao) or fimVigencia is null
    order by route_id, fimVigencia
)
select 
    *
from ultimas_versoes

