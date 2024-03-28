{{
  config(
    materialized="incremental",
    partition_by={
      "field": "data", 
      "data_type": "date", 
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite",
  )
}}

/*
- status
  Classificação do veículo, observados os demais parâmetros - Categorias:
  Não classificado - Veículo sem classificação
  Não licenciado/Nao licenciado - Veículo que operou, mas não é licenciado
  Autuado por segurança - Veículo que operou, foi licenciado, mas foi autuado por infração relacionada à segurança do veículo
  Autuado por ar inoperante/Licenciado com ar e autuado (023.II) - Veículo que operou, foi licenciado com ar condicionado e foi autuado por inoperância ou mau funcionamento do sistema de ar condicionado (023.II)
  Autuado por limpeza/equipamento - Veículo que operou, foi licenciado, mas foi autuado cumulativamente por infrações relacionadas à limpeza e equipamentos do veículo
  Licenciado com ar e não autuado - Veículo que operou, foi licenciado com ar condicionado e não foi autuado
  Licenciado sem ar e não autuado/Licenciado sem ar - Veículo que operou, foi licenciado sem ar condicionado e não foi autuado
  Registrado com ar inoperante - Veículo que operou, foi licenciado com ar condicionado e foi registrado por inoperância ou mau funcionamento do sistema de ar condicionado por agente de verão (RESOLUÇÃO SMTR Nº 3.682/2024)
  Não vistoriado - Veículo que operou, mas não foi vistoriado tempestivamente conforme calendário de vistoria"
*/

WITH
  sppo_licenciamento_vistoria_historico AS (
    SELECT
      id_veiculo,
      placa,
      data_inicio_vinculo,
      MAX(data_inicio_periodo_vistoria) AS data_inicio_periodo_vistoria
    FROM
      {{ ref("sppo_licenciamento_vistoria_historico") }} 
    WHERE
      timestamp_primeira_captura <= DATETIME_ADD("{{ var('run_date') }} 00:00:00", INTERVAL 5 DAY)
      -- Alteração temporária para inclusão de veículos com data_ultima_vistoria em mar/2024
      OR ((DATE("{{ var('run_date') }}") BETWEEN "2024-03-01" AND "2024-03-31") 
          AND timestamp_primeira_captura <= DATETIME("2024-04-01 00:00:00"))
    GROUP BY
      1,
      2,
      3
  ),
  licenciamento AS (
    SELECT
      DATE("{{ var('run_date') }}") AS data,
      l.id_veiculo,
      l.placa,
      tipo_veiculo,
      indicador_ar_condicionado,
      TRUE AS indicador_licenciado,
      CASE
        WHEN DATE("{{ var('run_date') }}") < "2024-03-01" THEN TRUE -- Apesar do indicador passar a ser considerado apenas após 2024-03-01, será considerado TRUE nos casos anteriores
        WHEN data_ultima_vistoria IS NULL AND DATE_DIFF(DATE("{{ var('run_date') }}"), l.data_inicio_vinculo, DAY) <= 10 THEN TRUE -- Tolerância de 10 dias para vistoria inicial
        WHEN v.data_inicio_periodo_vistoria > "2022-12-31" THEN TRUE -- Apenas veículos com data_ultima_vistoria a partir de 2023 serão considerados válidos
        WHEN s.id_veiculo IS NOT NULL THEN TRUE -- Veículos com solicitação de vistoria pendente (exceção)
      ELSE
        FALSE
      END AS indicador_vistoriado
    FROM
      {{ ref("sppo_licenciamento") }} AS l
    LEFT JOIN
      {{ ref("sppo_licenciamento_vistoria_solicitacao") }} AS s
    USING
      (id_veiculo, placa)
    LEFT JOIN
      sppo_licenciamento_vistoria_historico AS v
    ON
      l.id_veiculo = v.id_veiculo
      AND l.placa = v.placa
      AND l.data_inicio_vinculo = v.data_inicio_vinculo
    WHERE
      (l.data_inicio_vinculo IS NULL
        OR l.data_inicio_vinculo <= DATE("{{ var('run_date') }}"))
      AND (data_fim_vinculo IS NULL
        OR data_fim_vinculo >= DATE("{{ var('run_date') }}"))
      AND l.timestamp_primeira_captura <= DATETIME_ADD("{{ var('run_date') }} 00:00:00", INTERVAL 5 DAY)
  ),
  gps AS (
  SELECT
    DISTINCT data,
    id_veiculo
  FROM
    {{ ref("gps_sppo") }} 
    --`rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
  WHERE
    data = DATE("{{ var('run_date') }}")),
  autuacoes AS (
  SELECT
    DISTINCT data_infracao AS data,
    placa,
    id_infracao
  FROM
    {{ ref("sppo_infracao") }}
  WHERE
  {%- if execute %}
    {% set infracao_date = run_query("SELECT MIN(data) FROM " ~ ref("sppo_infracao") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 7 DAY)").columns[0].values()[0] %}
  {% endif -%}
    data = DATE("{{ infracao_date }}")
    AND data_infracao = DATE("{{ var('run_date') }}")
    AND modo = "ONIBUS"),
  registros_agente_verao AS (
    SELECT
      DISTINCT data,
      id_veiculo,
      TRUE AS indicador_registro_agente_verao_ar_condicionado
    FROM
      {{ ref("sppo_registro_agente_verao") }}
    WHERE
      data = DATE("{{ var('run_date') }}") ),
  autuacao_ar_condicionado AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_ar_condicionado
  FROM
    autuacoes
  WHERE
    id_infracao = "023.II" ),
  autuacao_seguranca AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_seguranca
  FROM
    autuacoes
  WHERE
    id_infracao IN (
      "016.VI",
      "023.VII",
      "024.II",
      "024.III",
      "024.IV",
      "024.V",
      "024.VI",
      "024.VII",
      "024.VIII",
      "024.IX",
      "024.XII",
      "024.XIV",
      "024.XV",
      "025.II",
      "025.XII",
      "025.XIII",
      "025.XIV",
      "026.X") ),
  autuacao_equipamento AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_equipamento
  FROM
    autuacoes
  WHERE
    id_infracao IN (
      "023.IV",
      "023.V",
      "023.VI",
      "023.VIII",
      "024.XIII",
      "024.XI",
      "024.XVIII",
      "024.XXI",
      "025.III",
      "025.IV",
      "025.V",
      "025.VI",
      "025.VII",
      "025.VIII",
      "025.IX",
      "025.X",
      "025.XI") ),
  autuacao_limpeza AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_limpeza
  FROM
    autuacoes
  WHERE
    id_infracao IN (
      "023.IX",
      "024.X") ),
  autuacoes_agg AS (
  SELECT
    DISTINCT *
  FROM
    autuacao_ar_condicionado
  FULL JOIN
    autuacao_seguranca
  USING
    (data,
      placa)
  FULL JOIN
    autuacao_equipamento
  USING
    (data,
      placa)
  FULL JOIN
    autuacao_limpeza
  USING
    (data,
      placa) ),
  gps_licenciamento_autuacao AS (
  SELECT
    data,
    id_veiculo,
    STRUCT( COALESCE(l.indicador_licenciado, FALSE)                     AS indicador_licenciado,
            COALESCE(l.indicador_vistoriado, FALSE)                     AS indicador_vistoriado,
            COALESCE(l.indicador_ar_condicionado, FALSE)                AS indicador_ar_condicionado,
            COALESCE(a.indicador_autuacao_ar_condicionado, FALSE)       AS indicador_autuacao_ar_condicionado,
            COALESCE(a.indicador_autuacao_seguranca, FALSE)             AS indicador_autuacao_seguranca,
            COALESCE(a.indicador_autuacao_limpeza, FALSE)               AS indicador_autuacao_limpeza,
            COALESCE(a.indicador_autuacao_equipamento, FALSE)           AS indicador_autuacao_equipamento,
            COALESCE(r.indicador_registro_agente_verao_ar_condicionado, FALSE)   AS indicador_registro_agente_verao_ar_condicionado) AS indicadores
  FROM
    gps g
  LEFT JOIN
    licenciamento AS l
  USING
    (data,
      id_veiculo)
  LEFT JOIN
    autuacoes_agg AS a
  USING
    (data,
      placa)
  LEFT JOIN
    registros_agente_verao AS r
  USING
    (data,
      id_veiculo))
SELECT
  gla.* EXCEPT(indicadores),
  TO_JSON(indicadores) AS indicadores,
  status,
  "{{ var("version") }}" AS versao
FROM
  gps_licenciamento_autuacao AS gla
LEFT JOIN
  {{ ref("subsidio_parametros") }} AS p
  --`rj-smtr.dashboard_subsidio_sppo.subsidio_parametros` AS p
ON
  gla.indicadores.indicador_licenciado = p.indicador_licenciado
  AND gla.indicadores.indicador_vistoriado = p.indicador_vistoriado
  AND gla.indicadores.indicador_ar_condicionado = p.indicador_ar_condicionado
  AND gla.indicadores.indicador_autuacao_ar_condicionado = p.indicador_autuacao_ar_condicionado
  AND gla.indicadores.indicador_autuacao_seguranca = p.indicador_autuacao_seguranca
  AND gla.indicadores.indicador_autuacao_limpeza = p.indicador_autuacao_limpeza
  AND gla.indicadores.indicador_autuacao_equipamento = p.indicador_autuacao_equipamento
  AND gla.indicadores.indicador_registro_agente_verao_ar_condicionado = p.indicador_registro_agente_verao_ar_condicionado
  AND (data BETWEEN p.data_inicio AND p.data_fim)