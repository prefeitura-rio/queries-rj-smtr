{{ 
config(
    partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    },
)
}}

WITH
  remun_tarifa AS (
  SELECT DISTINCT
    data,
    CASE
      WHEN LENGTH(linha) < 3 THEN LPAD(linha, 3, "0")
    ELSE
    CONCAT( IFNULL(REGEXP_EXTRACT(linha, r'[B-Z]+'), ""), IFNULL(REGEXP_EXTRACT(linha, r'[0-9]+'), "") )
  END
    AS servico,
    SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie) AS remuneracao_tarifaria
  FROM
    {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }}
  WHERE
    data BETWEEN DATE('2022-06-01') 
    AND DATE('2023-05-31') 
    and data NOT IN ('2022-10-02', '2022-10-30', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')    
  GROUP BY
    1,2
),
remun_subsidio AS (
    SELECT
        extract(year from data) AS ano,
        s.data,
        s.consorcio,
        s.servico,
        sum(ifnull(remuneracao_tarifaria, 0)) AS remuneracao_tarifaria,
        sum(ifnull(subsidio, 0)) AS subsidio
    FROM
      {{ ref("servico_dia") }} s
    LEFT JOIN
      remun_tarifa
    USING
        (data, servico)
    GROUP BY 1,2,3,4
),

-- 1.2 Tabela com dados da receita aferida
receita_aferida AS (
SELECT 
    *,
    ROUND(remuneracao_tarifaria + subsidio, 2) AS receita_aferida
FROM 
    remun_subsidio
),


-- 2. Receita Esperada 
-- 2.1 Calcula remuneração esperada por tipo de viagem
   km_tipo_viagem AS (
    SELECT DISTINCT
      data,
      servico,
      tipo_viagem,
      km_apurada AS quilometragem,
    FROM
    {{ ref("sumario_servico_tipo_viagem_dia") }}
    WHERE
      data BETWEEN DATE('2022-06-01') 
        AND DATE('2023-05-31') 
      AND data NOT IN ('2022-10-02', '2022-10-30', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')
    ),

irk_tipo_viagem AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    -- TODO: corrigir tipos na tabela de parametros
    CASE
      -- WHEN status = "Nao licenciado" THEN "Não licenciado"
      WHEN status = "Licenciado com ar e autuado (023.II)" THEN "Autuado por ar inoperante"
      WHEN status = "Licenciado sem ar" THEN "Licenciado sem ar e não autuado"
      WHEN status = "Licenciado com ar e não autuado (023.II)" THEN "Licenciado com ar e não autuado"
      ELSE status
  END
    AS tipo_viagem,
    irk,
    irk_tarifa_publica,
    desconto_subsidio_km
  FROM
    {{ source("projeto_subsidio_sppo_encontro_contas", "subsidio_parametros_atualizada") }}
  WHERE
    data_fim >= DATE('2022-06-01') 
    AND data_inicio <= DATE('2023-05-31')

    -- Remove quilometragem irregular
    AND status != "Não licenciado"
    -- AND DATE("2023-01-18") BETWEEN data_inicio AND data_fim
    ),
-- 2.2 Tabela com dados da receita esperada
  receita_esperada AS (
    SELECT DISTINCT
      EXTRACT(year FROM s.data) AS ano,
      s.data,
      s.consorcio,
      s.servico,
      i.irk,
      i.irk_tarifa_publica,
      viagens,
      SUM(k.quilometragem) AS quilometragem,
      SUM(i.desconto_subsidio_km * k.quilometragem) AS desconto_subsidio,
      i.irk * SUM(k.quilometragem) AS receita_esperada
    FROM
      {{ ref("servico_dia") }} s
    LEFT JOIN
      km_tipo_viagem AS k
    USING
      (DATA,
        servico)
    INNER JOIN
      irk_tipo_viagem AS i
    ON
      k.tipo_viagem = i.tipo_viagem
      AND s.data BETWEEN i.data_inicio
      AND i.data_fim
    GROUP BY 1,2,3,4,5,6,7
  )
-- 3. Tabela com os dados do encontro de contas
    SELECT
    re.ano,
    re.data,
    re.consorcio,
    re.servico,
    irk,
    irk_tarifa_publica,
    viagens,
    quilometragem,
    desconto_subsidio,
    receita_esperada,
    (quilometragem * (irk - irk_tarifa_publica)) AS subsidio_esperado, 
    (quilometragem * irk_tarifa_publica) AS receita_tarifaria_esperada,  
    remuneracao_tarifaria AS receita_tarifaria,
    subsidio,
    receita_aferida,
    ROUND((remuneracao_tarifaria - (receita_esperada - desconto_subsidio)), 2) AS diff_tarifario_esperado,
    ROUND((receita_aferida - (receita_esperada - desconto_subsidio)), 2) AS diff_aferido_esperado    
    FROM receita_esperada AS re
    JOIN receita_aferida AS ra 
    ON re.data = ra.data AND re.servico = ra.servico