SELECT
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
  SAFE_CAST(id_veiculo AS STRING) id_veiculo,
  SAFE_CAST(JSON_VALUE(content,"$.placa") AS STRING) placa,
FROM
  {{ source("veiculo_staging", "sppo_licenciamento_vistoria_solicitacao") }} AS t
WHERE
  data = (SELECT MAX(data) FROM {{ source("veiculo_staging", "sppo_licenciamento_vistoria_solicitacao") }} WHERE SAFE_CAST(data AS DATE) >= DATE_ADD(DATE("{{ var('run_date') }}"), INTERVAL 5 DAY))