{{ 
config(
    alias='sumario_dia'
)
}}

-- Calcula percentual de KM executado de todos os serviços para cada dia, 
-- desde o início do subsídio (jun/22) até a data máxima da última quinzena apurada.

-- 1. Soma viagens realizadas de diferentes sentidos do mesmo serviço. 
-- A KM planejada é por serviço e nao sentido, portanto a distancia_total_planejada 
-- da sumario_periodo já é a total do serviço para ambos os sentidos e por isso nao somamos.
WITH
  sumario AS (
  SELECT
    consorcio,
    data,
    tipo_dia,
    servico,
    ROUND(SUM(viagens_planejadas), 3) AS viagens_planejadas,
    ROUND(SUM(viagens_subsidio), 3) AS viagens_subsidio,
    MAX(distancia_total_planejada) AS distancia_total_planejada,
    ROUND(SUM(distancia_total_subsidio), 3) AS distancia_total_subsidio,
  FROM
    {{ ref("sumario_subsidio_dia_periodo") }}
  GROUP BY
    1,
    2,
    3,
    4 ),
-- 2. Recupera o valor do KM para cada data planejada e calcula o percentual de KM executado. 
-- No caso de serviços sem planejamento para o dia, o percentual padrao é nulo.
  valor AS (
  SELECT
    s.*,
    v.valor_subsidio_por_km,
    ROUND(distancia_total_subsidio * v.valor_subsidio_por_km, 2) AS valor_total_aferido,
  IF
    (distancia_total_planejada = 0, NULL, ROUND(100*distancia_total_subsidio/distancia_total_planejada, 2)) AS perc_distancia_total_subsidio
  FROM
    sumario s
  LEFT JOIN (
    SELECT
      *
    FROM
      {{ ref("subsidio_data_versao_efetiva") }}
    WHERE
      data BETWEEN "2022-06-01" AND DATE("{{ var("end_date") }}")) AS v
  ON
    v.data = s.data )
-- 3. Define o valor total a ser pago com base no cumprimento do KM planejado.
SELECT
  *,
  CASE
    -- Serviços não programados para a data
    WHEN perc_distancia_total_subsidio IS NULL THEN 0
    {% if var("run_date") > "2023-01-16" -%}
        -- Penalidades segundo a resolução XX
        WHEN (perc_distancia_total_subsidio < {{ var("sppo_perc_distancia_penalidade_grave") }}) THEN {{ var("sppo_valor_penalidade_grave") }}
        WHEN (perc_distancia_total_subsidio < {{ var("sppo_perc_distancia_penalidade_media") }}) THEN {{ var("sppo_valor_penalidade_media") }}
    {%- endif %}
    -- Distância mínima para pagamento segundo a resolução XX
    WHEN perc_distancia_total_subsidio < {{ var("sppo_perc_distancia_minima_subsidio") }} THEN 0
  ELSE
  valor_total_aferido
END
  AS valor_total_subsidio
FROM
  valor
