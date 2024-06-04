/*
  Etapa de tratamento com base na resposta aos ofícios MTR-OFI-2024/03024, MTR-OFI-2024/03025, MTR-OFI-2024/03026 e MTR-OFI-2024/03027
*/

SELECT
    quinzena,
    PARSE_DATE("%m/%d/%Y", data_inicio_quinzena) AS data_inicio_quinzena,
    PARSE_DATE("%m/%d/%Y", data_final_quinzena) AS data_final_quinzena,
    consorcio_rdo,
    IF(LENGTH(servico_tratado_rdo) < 3, LPAD(servico_tratado_rdo, 3, "0"), servico_tratado_rdo) AS servico_tratado_rdo,
    IF(LENGTH(linha_rdo) < 3, LPAD(linha_rdo, 3, "0"), linha_rdo) AS linha_rdo,
    tipo_servico_rdo,
    ordem_servico_rdo,
    quantidade_dias_rdo,
    SAFE_CAST(REPLACE(REGEXP_REPLACE(receita_tarifaria_aferida_rdo , r"[^\d,-]", ""), ",", ".") AS FLOAT64) AS receita_tarifaria_aferida_rdo,
    justificativa,
    acao,
    CASE
      WHEN justificativa = "SVA665 a partir de 06/2022, SVB665 a partir de 08/2022" AND acao = "Considerar como SVB no período indicado" THEN "SVB665"
    ELSE
    REGEXP_EXTRACT(acao, "(?:[A-Z]+|)[0-9]+")
  END
    AS servico_corrigido_rioonibus,
FROM
    {{ source("projeto_subsidio_sppo_encontro_contas", "rdo_correcao_rioonibus_servico_quinzena") }}