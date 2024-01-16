{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}


WITH
  gps AS (
  SELECT
    DISTINCT
    data,
    id_veiculo
  FROM
    {{ ref("gps_sppo") }}
  WHERE
    data = DATE("2023-08-02T01:00:00") ),
  licenciamento AS (
  SELECT
    DATE("{{ var('run_date') }}") AS data,
    id_veiculo,
    placa,
    tipo_veiculo,
    indicador_ar_condicionado,
    TRUE AS indicador_licenciado
  FROM
    {{ ref("sppo_licenciamento") }}
  -- TODO (para pensar depois): tem como simplificar? deixar mais explicito qual data é valida para veiculos licenciados, possivelmente refatorar o tratamento do licenciamento ou centralizar todas as infos numa tabela única de veiculo
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
    and length(id_veiculo) = 6 -- exclui BRT
  ),
  autuacoes AS (
  SELECT
    data_infracao as data,
    placa,
    sum(case when id_infracao = "023.II" then 1 else 0 end) > 0 as indicador_autuacao_ar_condicionado,
    sum(case when id_infracao in ( -- TODO: transformar a lista em parametro (var)
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
      "026.X")
 then 1 else 0 end ) > 0 as indicador_autuacao_seguranca,
  sum(case when id_infracao in ( -- TODO: transformar a lista em parametro (var)
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
      "025.XI") then 1 else 0 end ) > 0 as indicador_autuacao_equipamento,
      -- TODO: transformar a lista em parametro (var)
      sum(case when id_infracao in ("023.IX", "024.X") then 1 else 0 end) > 0 as indicador_autuacao_limpeza,
  FROM
    {{ ref("sppo_infracao") }}
  -- TODO (para pensar depois): mesmo ponto do licenciamento
  WHERE
  {%- if execute %}
    {% set infracao_date = run_query("SELECT MIN(data) FROM " ~ ref("sppo_infracao") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 7 DAY)").columns[0].values()[0] %}
  {% endif -%}
    data = DATE("{{ infracao_date }}")
    AND data_infracao = DATE("{{ var('run_date') }}")
    AND modo = "ONIBUS"
  GROUP BY 
    placa, data
)
-- TODO: solucionar coluna de status, ta vindo nula!
SELECT
  COALESCE(g.data, l.data) as data,
  COALESCE(g.id_veiculo, l.id_veiculo) as id_veiculo,
  TO_JSON(STRUCT( 
    COALESCE(l.indicador_licenciado, FALSE) AS indicador_licenciado,
    COALESCE(l.indicador_ar_condicionado, FALSE) AS indicador_ar_condicionado,
    COALESCE(a.indicador_autuacao_ar_condicionado, FALSE) AS indicador_autuacao_ar_condicionado,
    COALESCE(a.indicador_autuacao_seguranca, FALSE) AS indicador_autuacao_seguranca,
    COALESCE(a.indicador_autuacao_limpeza, FALSE) AS indicador_autuacao_limpeza,
    COALESCE(a.indicador_autuacao_equipamento, FALSE) AS indicador_autuacao_equipamento
  )) AS indicadores,
  p.status,
  "" AS versao
FROM
  gps g
FULL JOIN
  licenciamento AS l
USING
  (id_veiculo, data)
FULL OUTER JOIN
  autuacoes AS a
ON
  l.data = a.data
  AND l.placa = a.placa
LEFT JOIN
  {{ ref("subsidio_parametros") }} AS p
ON
  g.data BETWEEN p.data_inicio AND p.data_fim
  AND l.indicador_licenciado = p.indicador_licenciado
  AND l.indicador_ar_condicionado = p.indicador_ar_condicionado
  AND a.indicador_autuacao_ar_condicionado = p.indicador_autuacao_ar_condicionado
  AND a.indicador_autuacao_seguranca = p.indicador_autuacao_seguranca
  AND a.indicador_autuacao_limpeza = p.indicador_autuacao_limpeza
  AND a.indicador_autuacao_equipamento = p.indicador_autuacao_equipamento