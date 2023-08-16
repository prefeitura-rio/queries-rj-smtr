



WITH
  -- 1. Recupera valor (subsidio pago e penalidade), km apurada e POD do dia
  sumario_dia AS (
  SELECT
    DATA,
    consorcio,
    servico,
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
    tipo_viagem,
    km_apurada
  FROM
    rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia
  WHERE
    DATA BETWEEN "2023-01-16"
    AND "2023-08-31"
    AND tipo_viagem != "Não licenciado"),
  -- 3. Calcula valor glosado do subsidio por tipo de viagem
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
    subsidio_km
  FROM
    rj-smtr.dashboard_subsidio_sppo.subsidio_parametros
  WHERE
    data_fim >= "2023-01-16"
    AND data_inicio <= "2023-08-31" ),

  -- 4. 
  subsidio_tipo_viagem AS (
  SELECT
    DATA,
    consorcio,
    servico,
    t.tipo_viagem,
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
    1, 2, 3, 4 )


SELECT DATA, servico,
/*
  -- km apurada
  SUM(CASE WHEN tipo_viagem = 'Sem viagem apurada' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_sem_viagem_apurada`,
  SUM(CASE WHEN tipo_viagem = 'Licenciado com ar e não autuado' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_com_ar_nao_autuado`,
  SUM(CASE WHEN tipo_viagem = 'Licenciado sem ar e não autuado' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_sem_ar_nao_autuado`,
  SUM(CASE WHEN tipo_viagem = 'Autuado por ar inoperante' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_autuado_por_ar_inoperante`,
  SUM(CASE WHEN tipo_viagem = 'Autuado por segurança' THEN km_apurada_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `km_autuado_por_seguranca`,
  -- valor do subsídio
  SUM(CASE WHEN tipo_viagem = 'Sem viagem apurada' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_sem_viagem_apurada`,
  SUM(CASE WHEN tipo_viagem = 'Licenciado com ar e não autuado' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_com_ar_nao_autuado`,
  SUM(CASE WHEN tipo_viagem = 'Licenciado sem ar e não autuado' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_sem_ar_nao_autuado`,
  SUM(CASE WHEN tipo_viagem = 'Autuado por ar inoperante' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_autuado_por_ar_inoperante`,
  SUM(CASE WHEN tipo_viagem = 'Autuado por segurança' THEN valor_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `subsidio_autuado_por_seguranca`,
  */
  -- glosa
  SUM(CASE WHEN tipo_viagem = 'Sem viagem apurada' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_sem_viagem_apurada`,
  SUM(CASE WHEN tipo_viagem = 'Licenciado com ar e não autuado' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_com_ar_nao_autuado`,
  SUM(CASE WHEN tipo_viagem = 'Licenciado sem ar e não autuado' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_sem_ar_nao_autuado`,
  SUM(CASE WHEN tipo_viagem = 'Autuado por ar inoperante' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_autuado_por_ar_inoperante`,
  SUM(CASE WHEN tipo_viagem = 'Autuado por segurança' THEN glosa_subsidio_tipo_viagem ELSE COALESCE(NULL, 0) END) AS `glosa_autuado_por_seguranca`

FROM subsidio_tipo_viagem
  GROUP BY
   1, 2




-- não achei autuado por limpeza e equipamento nas categorias (ainda não teve registro)
-- CONTINUAR DAQUI!
-- Entender o código abaixo e adicionar acima.
-- Fazer check valor total com o total da tabela do Rodrigo (sumario_subsidio_dia_com_glosas em produção)

/*

SELECT
  DATA,
  consorcio,
  servico,
  tipo_viagem,
  perc_km_planejada,
  km_apurada, -- Considera veículos não licenciados (continuam sem subsídio)
  km_apurada_tipo_viagem, -- Desconsidera veículos não licenciados
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
*/
  