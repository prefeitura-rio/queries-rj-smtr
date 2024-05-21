WITH
  consorcios AS (
  SELECT
    id_consorcio,
    case when id_consorcio = "221000050" then "Consórcio BRT" else consorcio end as consorcio
  FROM
    -- rj-smtr.cadastro.consorcios
    {{ ref("consorcios") }} )
SELECT
  data,
  ano,
  mes,
  dia,
  id_consorcio,
  consorcio,
  linha AS servico,
  r.* EXCEPT(data,
    ano,
    mes,
    dia,
    termo),
  (qtd_grt_idoso + qtd_grt_especial + qtd_grt_estud_federal + qtd_grt_estud_estadual + qtd_grt_estud_municipal + qtd_grt_rodoviario + qtd_buc_1_perna + qtd_buc_2_perna_integracao + qtd_buc_supervia_1_perna + qtd_buc_supervia_2_perna_integracao + qtd_cartoes_perna_unica_e_demais + qtd_pagamentos_especie + qtd_grt_passe_livre_universitario) AS qtd_passageiros_total
FROM
  {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }} r
LEFT JOIN
  consorcios c
ON
  r.termo = c.id_consorcio