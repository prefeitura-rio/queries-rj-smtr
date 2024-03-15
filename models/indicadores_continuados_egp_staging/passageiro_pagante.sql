{{
  config( 
    partition_by = { 
    "field": "data",
    "data_type": "date",
    "granularity": "month"
    },
)}}

WITH consorcio AS (
    SELECT
    id_consorcio,
    modo
  FROM
    {{ ref("consorcios") }} AS c
  LEFT JOIN
    {{ ref("operadoras") }} AS o
  ON
    c.id_consorcio_jae = o.id_operadora_jae
  WHERE
    consorcio IN ("MobiRio", "Internorte", "Intersul", "Santa Cruz", "Transcarioca")
)
SELECT
  DATE_TRUNC(data, MONTH) AS data,
  rdo.ano,
  rdo.mes,
  c.modo,
  SUM(qtd_buc_1_perna+qtd_buc_2_perna_integracao+
      qtd_buc_supervia_1_perna+qtd_buc_supervia_2_perna_integracao+
      qtd_cartoes_perna_unica_e_demais+qtd_pagamentos_especie) AS quantidade_passageiro_pagante_mes,
  CURRENT_DATE() AS data_ultima_atualizacao,
  '{{ var("version") }}' as versao
FROM
  consorcio AS c
LEFT JOIN
  {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }} AS rdo
ON
  rdo.termo = c.id_consorcio
WHERE 
  rdo.data >= "2015-01-01"
  {% if is_incremental() %}
  AND rdo.data BETWEEN DATE_TRUNC(DATE("{{ var("start_date") }}"), MONTH)
  AND LAST_DAY(DATE("{{ var("end_date") }}"), MONTH)
  AND rdo.data < DATE_TRUNC(CURRENT_DATE(), MONTH)
  {% endif %}
GROUP BY 
  data, 
  rdo.ano, 
  rdo.mes, 
  c.modo