{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

WITH
  licenciamento AS (
  SELECT
    DATE("{{ var('run_date') }}") AS data,
    id_veiculo,
    placa,
    tipo_veiculo,
    indicador_ar_condicionado,
    TRUE AS indicador_licenciado
  FROM
    {{ ref("sppo_licenciamento") }} --`rj-smtr`.`veiculo`.`sppo_licenciamento`
  {%- if var("stu_data_versao") != "" %}
  WHERE 
    data = DATE("{{ var('stu_data_versao') }}")
  {% else -%}
    {%- if execute %}
        {% set licenciamento_date = run_query("SELECT MIN(data) FROM " ~ ref("sppo_licenciamento") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 5 DAY)").columns[0].values()[0] %}
    {% endif -%}
  WHERE 
    data = DATE("{{ licenciamento_date }}")
  {% endif -%}  
  ),
  gps AS (
  SELECT
    DISTINCT data,
    id_veiculo
  FROM
    {{ ref("gps_sppo") }}
  WHERE
    data = DATE("{{ var('run_date') }}") ),
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
    STRUCT( COALESCE(l.indicador_licenciado, FALSE) AS indicador_licenciado,
      COALESCE(l.indicador_ar_condicionado, FALSE) AS indicador_ar_condicionado,
      COALESCE(a.indicador_autuacao_ar_condicionado, FALSE) AS indicador_autuacao_ar_condicionado,
      COALESCE(a.indicador_autuacao_seguranca, FALSE) AS indicador_autuacao_seguranca,
      COALESCE(a.indicador_autuacao_limpeza, FALSE) AS indicador_autuacao_limpeza,
      COALESCE(a.indicador_autuacao_equipamento, FALSE) AS indicador_autuacao_equipamento) AS indicadores
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
      placa))
SELECT
  gla.* EXCEPT(indicadores),
  TO_JSON(indicadores) AS indicadores,
  status,
  "{{ var("version") }}" AS versao
FROM
  gps_licenciamento_autuacao AS gla
LEFT JOIN
  {{ ref("subsidio_parametros") }} AS p --`rj-smtr.dashboard_subsidio_sppo.subsidio_parametros` 
ON
  gla.indicadores.indicador_licenciado = p.indicador_licenciado
  AND gla.indicadores.indicador_ar_condicionado = p.indicador_ar_condicionado
  AND gla.indicadores.indicador_autuacao_ar_condicionado = p.indicador_autuacao_ar_condicionado
  AND gla.indicadores.indicador_autuacao_seguranca = p.indicador_autuacao_seguranca
  AND gla.indicadores.indicador_autuacao_limpeza = p.indicador_autuacao_limpeza
  AND gla.indicadores.indicador_autuacao_equipamento = p.indicador_autuacao_equipamento
  AND (data BETWEEN p.data_inicio AND p.data_fim)