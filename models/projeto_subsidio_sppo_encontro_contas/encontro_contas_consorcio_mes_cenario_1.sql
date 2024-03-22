-- depends_on: {{ ref('recursos_sppo_servico_dia_pago') }}
{{ config(
  materialized = "table",
) }}

{% if execute %}
  {% set lista_datas_remover = run_query("SELECT DISTINCT DATA FROM " ~ ref('recursos_sppo_servico_dia_pago') ~ " WHERE servico = 'Todos'").columns[0].values() %}
{% endif %}

{% set data_inicio = "2022-06-01" %}
{% set data_fim = "2023-12-31" %}

WITH
  recurso AS (
  SELECT
    DISTINCT DATA,
    servico
  FROM
    {{ ref("encontro_contas_recursos_sppo_servico_dia_pago") }} ),
  servico_dia_subsidio AS (
    SELECT
      data,
      servico,
    FROM
      --{{ ref("sumario_servico_dia_historico") }}
      `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
    WHERE
      perc_km_planejada >= 80
  ),
  subsidio AS (
  SELECT
    s.*
  FROM (
    SELECT
      EXTRACT(year
      FROM
        DATA) AS ano,
      EXTRACT(MONTH
      FROM
        DATA) AS mes,
      DATA,
      consorcio,
      servico,
      km_apurada AS km_apurada,
      valor_subsidio_pago AS subsidio,
      (irk * km_apurada) AS receita_esperada,
    FROM
      --{{ ref("sumario_servico_dia_historico") }}
      `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
    LEFT JOIN
      (
        SELECT DISTINCT
          data_inicio,
          data_fim,
          irk
        FROM
          {{ source("projeto_subsidio_sppo_encontro_contas", "subsidio_parametros_atualizada") }}
      ) AS p
    ON
    (data BETWEEN p.data_inicio
      AND p.data_fim)
    LEFT JOIN
      servico_dia_subsidio AS sd
    USING
      (data,
        servico)
    WHERE
      DATA BETWEEN "{{ data_inicio }}"
      AND "{{ data_fim }}"
      AND DATA NOT IN ("2022-10-02",
        "2022-10-30",
        '{{ lista_datas_remover|join("', '") }}') 
      AND sd.servico IS NOT NULL) s
  LEFT JOIN
    recurso r
  USING
    (DATA,
      servico)
  WHERE
    r.data IS NULL),
  desconto AS (
  SELECT
    EXTRACT(year
    FROM
      DATA) AS ano,
    EXTRACT(MONTH
      FROM
        DATA) AS mes,
    s.data,
    s.consorcio,
    s.servico,
    SUM(s.km_apurada * desconto_subsidio_km) AS desconto
  FROM (
    SELECT
      DATA,
      consorcio,
      servico,
      tipo_viagem,
      km_apurada
    FROM
      --{{ ref("sumario_servico_tipo_viagem_dia") }}
      `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_tipo_viagem_dia`
    WHERE
      DATA BETWEEN "{{ data_inicio }}"
      AND "{{ data_fim }}") s
  LEFT JOIN (
    SELECT
      DISTINCT data_inicio,
      data_fim,
      CASE
        WHEN status = "Licenciado com ar e autuado (023.II)" THEN "Autuado por ar inoperante"
        WHEN status = "Licenciado sem ar" THEN "Licenciado sem ar e não autuado"
        WHEN status = "Licenciado com ar e não autuado (023.II)" THEN "Licenciado com ar e não autuado"
      ELSE
      status
    END
      AS tipo_viagem,
      irk,
      irk_tarifa_publica,
      desconto_subsidio_km
    FROM
      {{ source("projeto_subsidio_sppo_encontro_contas", "subsidio_parametros_atualizada") }} ) p
  ON
    (s.data BETWEEN p.data_inicio
      AND p.data_fim)
    AND s.tipo_viagem = p.tipo_viagem
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
  esperado AS (
  SELECT
    s.ano,
    s.mes,
    s.consorcio,
    SUM(km_apurada) AS km,
    SUM(subsidio) AS subsidio,
    SUM(receita_esperada) AS receita_esperada,
    SUM(desconto) AS desconto
  FROM
    subsidio s
  LEFT JOIN
    recurso r
  USING
    (DATA,
      servico)
  LEFT JOIN
    desconto d
  USING
    (DATA,
      servico)
  GROUP BY
    1,
    2,
    3 ),
  rdo AS (
  SELECT
    rdo.ano,
    rdo.mes,
    c.consorcio,
    SUM(rdo.remuneracao_tarifaria) AS remuneracao_tarifaria
  FROM (
    SELECT
      EXTRACT(year
      FROM
        DATA) AS ano,
      EXTRACT(MONTH
      FROM
        DATA) AS mes,
      DATA,
      termo AS id_consorcio,
      CASE
        WHEN LENGTH(linha) < 3 THEN LPAD(linha, 3, "0")
      ELSE
      CONCAT( IFNULL(REGEXP_EXTRACT(linha, r"[B-Z]+"), ""), IFNULL(REGEXP_EXTRACT(linha, r"[0-9]+"), "") )
    END
      AS servico,
      SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie) AS remuneracao_tarifaria
    FROM
      {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }}
    WHERE
      DATA BETWEEN DATE("{{ data_inicio }}")
      AND DATE("{{ data_fim }}")
      AND DATA NOT IN ("2022-10-02",
        "2022-10-30",
        '{{ lista_datas_remover|join("', '") }}')
    GROUP BY
      1,
      2,
      3,
      4,
      5 ) rdo
  INNER JOIN (
    SELECT
      consorcio,
      id_consorcio
    FROM
      {{ ref("consorcios") }}
    WHERE
      consorcio IN ("Internorte",
        "Intersul",
        "Santa Cruz",
        "Transcarioca") ) c
  USING
    (id_consorcio)
  LEFT JOIN
    recurso r
  USING
    (DATA,
      servico)
  LEFT JOIN
    servico_dia_subsidio AS sd
  USING
    (data,
      servico)  
  WHERE
    r.data IS NULL
    AND sd.servico IS NOT NULL
  GROUP BY
    1,
    2,
    3 )
SELECT
  ano,
  mes,
  consorcio,
  ROUND(km/POW(10,6), 2) km_milhoes,
  ROUND(remuneracao_tarifaria/POW(10,6), 2) remuneracao_tarifaria_milhoes,
  ROUND(subsidio/POW(10,6), 2) subsidio_milhoes,
  ROUND(receita_aferida/POW(10,6), 2) AS receita_aferida_milhoes,
  ROUND(receita_esperada/POW(10,6), 2) AS receita_esperada_milhoes,
  ROUND(desconto/POW(10,6), 2) AS desconto_milhoes,
  ROUND((receita_aferida - receita_esperada + desconto)/POW(10, 6), 2) AS saldo
FROM (
  SELECT
    e.*,
    rdo.remuneracao_tarifaria,
    rdo.remuneracao_tarifaria + e.subsidio AS receita_aferida,
  FROM
    esperado e
  FULL JOIN
    rdo
  USING
    ( ano,
      mes,
      consorcio) )
ORDER BY
  1,
  2