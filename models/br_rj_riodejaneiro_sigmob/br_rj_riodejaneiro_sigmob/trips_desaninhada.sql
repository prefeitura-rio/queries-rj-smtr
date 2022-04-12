select
  trip_id,
  json_value(content, "$.route_id") route_id,
  json_value(content, "$.service_id") service_id,
  json_value(content, "$.trip_headsign") trip_headsign,
  json_value(content, "$.trip_short_name") trip_short_name,
  json_value(content, "$.direction_id") direction_id,
  json_value(content, "$.block_id") block_id,
  json_value(content, "$.shape_id") shape_id,
  json_value(content, "$.variacao_itinerario") variacao_itinerario,
  json_value(content, "$.versao") versao,
  json_value(content, "$.complemento") complemento,
  json_value(content, "$.via") via,
  json_value(content, "$.observacoes") observacoes,
  json_value(content, "$.ultima_medicao_operante") ultima_medicao_operante,
  json_value(content, "$.idModalSmtr") id_modal_smtr,
  json_value(content, "$.Direcao") direcao,
  json_value(content, "$.id") id,
  DATE(data_versao) data_versao

from {{ ref('trips') }}