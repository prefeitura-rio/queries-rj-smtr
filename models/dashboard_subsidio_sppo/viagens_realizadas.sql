SELECT
  * EXCEPT(id_classificacao)
FROM
  --`rj-smtr.projeto_subsidio_sppo.viagem_completa` -- {{ ref("viagem_completa") }}
  `rj-smtr-dev.projeto_subsidio_sppo.viagem_completa_subsidio` -- {{ ref("viagem_completa") }}
WHERE
  data BETWEEN "2022-06-01" AND DATE("{{ var("end_date") }}")
  AND id_classificacao != 1