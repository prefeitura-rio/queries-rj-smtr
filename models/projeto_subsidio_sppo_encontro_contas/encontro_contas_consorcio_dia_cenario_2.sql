-- depends_on: {{ ref('recursos_sppo_servico_dia_pago') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

/* Cenário 2: Balanço da receita aferida e esperada por dia considerando recursos já pagos 
e recursos em avaliação pela TR/SUBTT/CMO (recursos_sppo_servico_dia_pago + recursos_sppo_servico_dia_avaliacao) */

-- Quando houver um recurso pago para todos os serviços, desconsiderar o dia
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
    (
      SELECT
        data,
        servico
      FROM
        {{ ref("encontro_contas_recursos_sppo_servico_dia_pago") }}
      UNION ALL
      SELECT
        data,
        servico
      FROM
        {{ ref("recursos_sppo_servico_dia_avaliacao") }}
    )
  ),
  servico_dia_subsidio AS (
    SELECT
      data,
      servico,
    FROM
      {{ ref("sumario_servico_dia_historico") }}
      --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
    WHERE
      perc_km_planejada >= 80
  ),
  subsidio AS (
  SELECT
    s.*
  FROM (
    SELECT
      DATA,
      consorcio,
      servico,
      km_apurada AS km_apurada,
      valor_subsidio_pago AS subsidio,
      (irk * km_apurada) AS receita_esperada,
    FROM
      {{ ref("sumario_servico_dia_historico") }}
      --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
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
      {{ ref("sumario_servico_tipo_viagem_dia") }}
      --`rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_tipo_viagem_dia`
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
    3 ),
  esperado AS (
  SELECT
    s.data,
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
    2 ),
  rdo AS (
  SELECT
    rdo.data,
    c.consorcio,
    SUM(rdo.remuneracao_tarifaria) AS remuneracao_tarifaria
  FROM (
    SELECT
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
      3 ) rdo
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
    2 )
SELECT
  data,
  consorcio,
  km,
  remuneracao_tarifaria,
  subsidio,
  receita_aferida,
  receita_esperada,
  desconto,
  (receita_aferida - receita_esperada + desconto) AS saldo
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
    (data,
      consorcio) )