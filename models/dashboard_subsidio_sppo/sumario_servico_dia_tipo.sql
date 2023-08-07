{% set status_list_query %}
WITH
  subsidio_parametros AS (
  SELECT
    *
  FROM
    {{ ref("subsidio_parametros") }}
  WHERE
    status != "Não classificado"
  ORDER BY
    data_inicio DESC,
    ordem),
  tabela_status_array AS (
  SELECT
    TO_JSON_STRING(STRUCT(indicador_licenciado,
        indicador_ar_condicionado,
        indicador_autuacao_ar_condicionado,
        indicador_autuacao_seguranca,
        indicador_autuacao_limpeza,
        indicador_autuacao_equipamento,
        indicador_sensor_temperatura,
        indicador_validador_sbd )) AS indicadores,
    ARRAY_AGG(status) AS status_array
  FROM
    subsidio_parametros
  GROUP BY
    indicadores),
    status_principal AS (
SELECT
  status_array[OFFSET(0)] AS status,
  LOWER(
    REGEXP_REPLACE(
      TRANSLATE(
        status_array[OFFSET(0)],
        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
      ),
      r'[^\w\s]', -- Remove caracteres não alfanuméricos e não espaços
      ''
    )
  ) AS status_tratado
FROM
  tabela_status_array)
SELECT DISTINCT
  status,
  REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(status_tratado, r'\b(e|por)\b', ''), -- Remove "e" e "por"
        r'\bnao\b', 'n'  -- Substitui "não" por "n"
      ),
  r'[_\s]+', '_' -- Substitui múltiplos espaços ou underscores por um único "_"
  ) AS status_tratado 
FROM
  status_principal
{% endset %}

WITH
  planejado AS (
  SELECT
    DISTINCT DATA,
    tipo_dia,
    consorcio,
    servico,
    distancia_total_planejada AS km_planejada
  FROM
    {{ ref("viagem_planejada") }}
  WHERE
    DATA BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE( "{{ var("end_date") }}" )
    AND (distancia_total_planejada > 0
    OR distancia_total_planejada IS NOT NULL) ),
  veiculos AS (
  SELECT
    DATA,
    id_veiculo,
    status
  FROM
    {{ ref("sppo_veiculo_dia") }}
  WHERE
    DATA BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE( "{{ var("end_date") }}" )),
  viagem AS (
  SELECT
    DATA,
    servico_realizado AS servico,
    id_veiculo,
    id_viagem,
    distancia_planejada
  FROM
    {{ ref("viagem_completa") }}
  WHERE
    DATA BETWEEN DATE( "{{ var("DATA_SUBSIDIO_V2_INICIO") }}" )
    AND DATE( "{{ var("end_date") }}" )),
  servico_km_tipo AS (
  SELECT
    v.DATA,
    v.servico,
    ve.status AS tipo_viagem,
    COUNT(id_viagem) AS viagens,
    ROUND(SUM(distancia_planejada), 2) AS km_apurada
  FROM
    viagem v
  LEFT JOIN
    veiculos ve
  ON
    ve.data = v.data
    AND ve.id_veiculo = v.id_veiculo
  GROUP BY
    1,
    2,
    3 ),
  subsidio_parametros AS (
  SELECT
    *
  FROM
    {{ ref("subsidio_parametros") }}
  WHERE
    status != "Não classificado"
  ORDER BY
    data_inicio DESC,
    ordem),
  tabela_status_array AS (
  SELECT
    TO_JSON_STRING(STRUCT(indicador_licenciado,
        indicador_ar_condicionado,
        indicador_autuacao_ar_condicionado,
        indicador_autuacao_seguranca,
        indicador_autuacao_limpeza,
        indicador_autuacao_equipamento,
        indicador_sensor_temperatura,
        indicador_validador_sbd )) AS indicadores,
    ARRAY_AGG(status) AS status_array
  FROM
    subsidio_parametros
  GROUP BY
    indicadores),
  status_update AS (
  SELECT
    indicadores,
    status_array,
    status_array[OFFSET(0)] AS status
  FROM
    tabela_status_array),
  status_flat AS (
  SELECT DISTINCT 
    status_t, 
    status 
  FROM 
    status_update, 
    UNNEST(status_array) AS status_t),
  servico_km_tipo_atualizado AS (
  SELECT
    k.* EXCEPT(tipo_viagem),
    u.status AS tipo_viagem
  FROM
    servico_km_tipo AS k
  LEFT JOIN
    status_flat AS u
  ON 
    u.status_t = k.tipo_viagem),
  servico_km AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.consorcio,
    p.servico,
    v.tipo_viagem,
    IFNULL(v.viagens, 0) AS viagens,
    IFNULL(v.km_apurada, 0) AS km_apurada,
  FROM
    planejado p
  LEFT JOIN
    servico_km_tipo_atualizado v
  ON
    p.data = v.data
    AND p.servico = v.servico ),
  pivot_data AS (
  SELECT
    *
  FROM (
    SELECT
      data,
      tipo_dia,
      consorcio,
      servico,
      tipo_viagem,
      viagens,
      km_apurada,
    FROM
      servico_km ) PIVOT(SUM(viagens) AS viagens,
      SUM(km_apurada) AS km_apurada FOR tipo_viagem IN (
        {% if execute %}
          {% set status_q = run_query(status_list_query) %}
          {% set status_list = status_q.columns[0].values() %}
          {% set status_treated_list = status_q.columns[1].values() %}
          {% for index in range(status_list|length) %}
            {% set status = status_list[index] %}
            {% set status_treated = status_treated_list[index] %}
            "{{ status }}" AS {{ status_treated }}{% if not loop.last %},{% endif %}
          {% endfor %}
        {% endif %}
        )))
SELECT
  sd.*,
  pd.* EXCEPT(data,
    tipo_dia,
    servico,
    consorcio)
FROM
  {{ ref("sumario_servico_dia") }} AS sd
LEFT JOIN
  pivot_data AS pd
ON
  sd.data = pd.data
  AND sd.servico = pd.servico
ORDER BY
  DATA,
  servico