SELECT
  service_id,
  JSON_VALUE(content, "$.monday") monday,
  JSON_VALUE(content, "$.tuesday") tuesday,
  JSON_VALUE(content, "$.wednesday") wednesday,
  JSON_VALUE(content, "$.thursday") thursday,
  JSON_VALUE(content, "$.friday") friday,
  JSON_VALUE(content, "$.saturday") saturday,
  JSON_VALUE(content, "$.sunday") sunday,
  JSON_VALUE(content, "$.start_date") start_date,
  JSON_VALUE(content, "$.end_date") end_date,
  DATE(data_versao) data_versao

FROM {{ ref('calendar') }}