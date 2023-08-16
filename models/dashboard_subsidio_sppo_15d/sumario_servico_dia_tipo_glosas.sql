/*
Arrumar os nomes das tabelas
valor_subsidio_tipo_viagem é o valor do subsídio para cada tipo de km (não inclui a regra dos 80%)  
*/

WITH
  -- 1. Recupera valor (subsidio pago e penalidade), km apurada e POD do dia
  sumario_dia AS (
  SELECT
    DATA,
    consorcio,
    servico,
    km_planejada, -- add 
    perc_km_planejada,
    km_apurada,
    ROUND(valor_subsidio_pago,2) AS valor_subsidio_pago,
    valor_penalidade
  FROM
    rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia
  WHERE
    DATA BETWEEN "2023-01-16"
    AND "2023-08-31" ),
  -- 2. Recupera km apurada por tipo de viagem
  sumario_tipo_viagem AS (
  SELECT
    DATA,
    consorcio,
    servico,
    viagens, --add
    tipo_viagem,
    km_apurada
  FROM
    rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia
  WHERE
    DATA BETWEEN "2023-01-16"
    AND "2023-08-31"
    AND tipo_viagem != "Não licenciado"),
  -- 2. valores dos parâmetros, como o subsídio por km
  parametros_tipo_viagem AS (
  SELECT
    data_inicio,
    data_fim,
    CASE
    -- WHEN status = "Nao licenciado" THEN "Não licenciado"
      WHEN status = "Licenciado com ar e autuado (023.II)" THEN "Autuado por ar inoperante"
      WHEN status = "Licenciado sem ar" THEN "Licenciado sem ar e não autuado"
      WHEN status = "Licenciado com ar e não autuado (023.II)" THEN "Licenciado com ar e não autuado"
    ELSE
    status
  END
    AS tipo_viagem,
    subsidio_km,
  FROM
    rj-smtr.dashboard_subsidio_sppo.subsidio_parametros
  WHERE
    data_fim >= "2023-01-16"
    AND data_inicio <= "2023-08-31" ),

  -- 3. Calcula valor glosado do subsidio por tipo de viagem
  subsidio_tipo_viagem AS (
  SELECT
    DATA,
    consorcio,
    servico,
    t.tipo_viagem, --add
    IFNULL(ROUND(SUM(t.viagens), 2), 0) AS viagens, --add
    ROUND(SUM(t.km_apurada),2) AS km_apurada_tipo_viagem,
    IFNULL(ROUND(SUM(t.km_apurada * p.subsidio_km), 2), 0) AS valor_subsidio_tipo_viagem,
    IFNULL(ROUND(SUM(t.km_apurada * (2.81 - p.subsidio_km)), 2), 0) AS glosa_subsidio_tipo_viagem
  FROM
    sumario_tipo_viagem t
  LEFT JOIN
    parametros_tipo_viagem p
  ON
    t.data BETWEEN p.data_inicio
    AND p.data_fim
    AND t.tipo_viagem = p.tipo_viagem
  GROUP BY
    1,
    2, 3, 4),

  -- 4. Calcula valores glosados
  tabela_glosados AS (
    SELECT
      DATA,
      consorcio,
      servico,
      tipo_viagem, --add
      viagens, --add
      km_planejada,
      perc_km_planejada,
      km_apurada, -- Considera veículos não licenciados (continuam sem subsídio)
      km_apurada_tipo_viagem, -- Desconsidera veículos não licenciados (é a que deve ser usada)
      (valor_subsidio_pago + valor_penalidade) as valor_subsidio_com_glosa,
      valor_subsidio_tipo_viagem,
      -- Quando há subsidio, a glosa é o pagamento diferenciado por km dos tipos de viagem
      CASE
        WHEN valor_subsidio_pago > 0 THEN glosa_subsidio_tipo_viagem
      ELSE
      0
    END
      AS glosa_subsidio_tipo_viagem,
      -- Quando não subsidio, a glosa é o não pagamento do subsidio + penalidade (< 40% ou <60%)
      CASE
        WHEN valor_subsidio_pago = 0 THEN round(- valor_penalidade, 2)
      ELSE
      0
    END
      AS glosa_penalidade
    FROM
      sumario_dia s
    INNER JOIN
      subsidio_tipo_viagem t
    USING
      (DATA, consorcio, servico)
  ),

  --5. tabela servico-dia
  tabela_servico_dia AS (
    
    SELECT
    DATA,
    consorcio,
    servico,
    ROUND(SUM(viagens),0) AS viagens,
    km_planejada,
    perc_km_planejada,
    km_apurada,
    valor_subsidio_com_glosa,
    glosa_penalidade,


    -- km apurada
      SUM(CASE WHEN tipo_viagem = 'Sem viagem apurada' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_sem_viagem_apurada`,
      SUM(CASE WHEN tipo_viagem = 'Licenciado com ar e não autuado' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_com_ar_nao_autuado`,
      SUM(CASE WHEN tipo_viagem = 'Licenciado sem ar e não autuado' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_sem_ar_nao_autuado`,
      SUM(CASE WHEN tipo_viagem = 'Autuado por ar inoperante' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_autuado_por_ar_inoperante`,
      SUM(CASE WHEN tipo_viagem = 'Autuado por segurança' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_autuado_por_seguranca`,
      -- add autuação por limpeza e equipamento

      -- valor do subsídio
      SUM(CASE WHEN tipo_viagem = 'Sem viagem apurada' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_sem_viagem_apurada`,
      SUM(CASE WHEN tipo_viagem = 'Licenciado com ar e não autuado' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_com_ar_nao_autuado`,
      SUM(CASE WHEN tipo_viagem = 'Licenciado sem ar e não autuado' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_sem_ar_nao_autuado`,
      SUM(CASE WHEN tipo_viagem = 'Autuado por ar inoperante' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_autuado_por_ar_inoperante`,
      SUM(CASE WHEN tipo_viagem = 'Autuado por segurança' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_autuado_por_seguranca`,
      -- add autuação por limpeza e equipamento

      -- glosa
      SUM(CASE WHEN tipo_viagem = 'Sem viagem apurada' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_sem_viagem_apurada`,
      SUM(CASE WHEN tipo_viagem = 'Licenciado com ar e não autuado' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_com_ar_nao_autuado`,
      SUM(CASE WHEN tipo_viagem = 'Licenciado sem ar e não autuado' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_sem_ar_nao_autuado`,
      SUM(CASE WHEN tipo_viagem = 'Autuado por ar inoperante' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_autuado_por_ar_inoperante`,
      SUM(CASE WHEN tipo_viagem = 'Autuado por segurança' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_autuado_por_seguranca`
      -- add autuação por limpeza e equipamento
      
      FROM tabela_glosados
      GROUP BY 1,2,3,5,6,7,8,9

  ),
  -- 6. tabela colunas novas
      tabela_3 AS (
        SELECT
          *,
          (km_sem_viagem_apurada + km_com_ar_nao_autuado + km_sem_ar_nao_autuado + km_autuado_por_ar_inoperante + km_autuado_por_seguranca) AS km_apurada_tipo_viagem, 
          (subsidio_sem_viagem_apurada + subsidio_com_ar_nao_autuado + subsidio_sem_ar_nao_autuado + subsidio_autuado_por_ar_inoperante + subsidio_autuado_por_seguranca) AS valor_subsidio_tipo_viagem,
          (glosa_sem_viagem_apurada + glosa_com_ar_nao_autuado + glosa_sem_ar_nao_autuado + glosa_autuado_por_ar_inoperante + glosa_autuado_por_seguranca) AS glosa_subsidio_tipo_viagem

      FROM tabela_servico_dia
      ),
  -- 7. calcular km paga e km paga excluindo autuações
      tabela_4 AS (
        SELECT
          *,
          CASE 
              WHEN perc_km_planejada >= 80 THEN km_apurada_tipo_viagem 
              ELSE 0 
          END AS km_paga, -- exclui a km das penalidades por 40 e 60%
          CASE 
              WHEN perc_km_planejada >= 80 THEN (km_apurada_tipo_viagem - (km_sem_viagem_apurada + km_autuado_por_ar_inoperante + km_autuado_por_seguranca))
              ELSE 0 
          END AS km_paga_excl_autuacoes, -- exclui a km das autuações por ar, segurança etc  

      FROM tabela_3
      ),
  -- 8. Calcular valor pago com e sem autuações
      tabela_5 AS (

        SELECT 
          DATA,
          consorcio,
          servico,
          viagens,
          km_planejada,
          perc_km_planejada,
          km_apurada,
          km_apurada_tipo_viagem,
          km_paga,
          km_paga_excl_autuacoes,
          (km_planejada * 2.81) AS valor_planejado,
          (km_apurada * 2.81) AS valor_apurado,
          (km_apurada_tipo_viagem * 2.81) AS valor_apurado_tipo_viagem, --exclui os veículos não licenciados - é o valor do subsidio cheio 2.81 * km_apurada (exclui km não identificada)
          CASE 
              WHEN perc_km_planejada >= 80 THEN (km_apurada_tipo_viagem * 2.81)
              ELSE 0 
          END AS valor_pago_sem_autacoes, -- não desconta valor das autuações por ar, segurança etc e considera apenas valores com POD >= 80%
          CASE 
              WHEN perc_km_planejada >= 80 THEN valor_subsidio_tipo_viagem
              ELSE 0 
          END AS valor_pago_com_autacoes, -- desconta valor das autuações por ar, segurança etc e considera apenas valores com POD >= 80%
          valor_subsidio_com_glosa,
          valor_subsidio_tipo_viagem,
          glosa_penalidade,
          glosa_subsidio_tipo_viagem,
          -- km apurada
          km_sem_viagem_apurada,
          km_com_ar_nao_autuado,
          km_sem_ar_nao_autuado,
          km_autuado_por_ar_inoperante,
          km_autuado_por_seguranca,
          -- add autuação por limpeza e equipamento
          -- valor do subsídio
          subsidio_sem_viagem_apurada,
          subsidio_com_ar_nao_autuado,
          subsidio_sem_ar_nao_autuado,
          subsidio_autuado_por_ar_inoperante,
          subsidio_autuado_por_seguranca,
          -- add autuação por limpeza e equipamento
          -- glosa
          glosa_sem_viagem_apurada,
          glosa_com_ar_nao_autuado,
          glosa_sem_ar_nao_autuado,
          glosa_autuado_por_ar_inoperante,
          glosa_autuado_por_seguranca
          -- add autuação por limpeza e equipamento
        FROM tabela_4
      )

      SELECT * FROM tabela_5
      WHERE
      DATA BETWEEN DATE('{{ var('data_inicio_quinzena') }}') AND DATE('{{ var('data_fim_quinzena') }}')

