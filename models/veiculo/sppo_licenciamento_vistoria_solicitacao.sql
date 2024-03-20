SELECT
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
  SAFE_CAST(id_veiculo AS STRING) id_veiculo,
  SAFE_CAST(JSON_VALUE(content,"$.placa") AS STRING) placa,
FROM
  {{ var("sppo_licenciamento_vistoria_solicitacao_staging") }}
WHERE
  data = (SELECT MAX(data) FROM {{ var("sppo_licenciamento_vistoria_solicitacao_staging") }} WHERE SAFE_CAST(data AS DATE) >= DATE_ADD(DATE("{{ var('run_date') }}"), INTERVAL 5 DAY))