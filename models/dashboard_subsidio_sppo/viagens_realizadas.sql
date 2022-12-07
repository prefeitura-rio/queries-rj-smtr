SELECT
  *
FROM
  {{ ref("viagem_completa") }}
WHERE
  data BETWEEN "2022-06-01" AND DATE_SUB(DATE("{{ var("run_date") }}"), INTERVAL 1 DAY)