SELECT
  *
FROM
  {{ ref("viagem_completa") }}
WHERE
  data BETWEEN "2022-06-01" AND DATE("{{ var("end_date") }}")