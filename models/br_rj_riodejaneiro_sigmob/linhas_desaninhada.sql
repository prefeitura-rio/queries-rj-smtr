SELECT
  linha_id,
  JSON_VALUE(content, "$.agency_id") agency_id,
  JSON_VALUE(content, "$.sigla") sigla,
  JSON_VALUE(content, "$.idModalSmtr") id_modal_smtr,
  JSON_VALUE(content, "$.TipoLinha") tipo_linha,
  JSON_VALUE(content, "$.NomeLinha") nome_linha,
  JSON_VALUE(content, "$.SiglaCompleta") sigla_completa,
  JSON_VALUE(content, "$.NumeroServicos") numero_servicos,
  JSON_VALUE(content, "$.ativa") ativa,
  JSON_VALUE(content, "$.FrotaDeterminada") frota_determinada,
  JSON_VALUE(content, "$.LegisFrotaDeterminada") legis_frota_determinada,
  JSON_VALUE(content, "$.FrotaOperante") frota_operante,
  JSON_VALUE(content, "$.id") id,
  JSON_VALUE(content, "$.Apresentacao") apresentacao,
  JSON_VALUE(content, "$.agency_name") agency_name,
  DATE(data_versao) data_versao
FROM {{ ref('linhas') }}