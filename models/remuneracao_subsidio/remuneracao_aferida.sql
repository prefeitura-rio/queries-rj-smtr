/* Remuneração aferida */
WITH
-- 1. Recupera serviços-dias subsidiados
servico_dia AS (
  SELECT DISTINCT
    DATE(data) AS data,
    consorcio,
    servico,
    valor_subsidio_pago as subsidio
  FROM
    {{ source('dashboard_subsidio_sppo', 'sumario_servico_dia_historico') }} 
  WHERE
    data between DATE('{{ var('data_inicio_subsidio') }}') AND DATE('{{ var('data_fim_subsidio') }}')
    AND data NOT IN ({{ var('datas_invalidas') }})
    AND valor_subsidio_pago > 0
),
-- 2. Remove serviços-dia pagos por recurso
recurso as (
    SELECT DISTINCT
       data_viagem as data,
       servico
    FROM
      {{ source('recurso_subsidio_sppo', 'reprocessamento') }} 

    WHERE
      data_viagem BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
                      AND DATE('{{ var('data_fim_subsidio') }}') 
    UNION ALL 
    SELECT DISTINCT
       data_viagem as data,
       servico
    FROM
      {{ source('recurso_subsidio_sppo', 'bloqueio_via') }}

    WHERE
      data_viagem BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
                      AND DATE('{{ var('data_fim_subsidio') }}') 
      AND DATE(data_viagem) NOT IN ( {{ var('datas_invalidas') }} )
    
),
servico_dia_valido as (
    SELECT
        s.*
    FROM
        servico_dia s
    LEFT JOIN 
        recurso r
    USING
        (data, servico)
    WHERE r.data is null
),
-- 3. Recupera receita diária do RDO para cada serviço
rdo AS (
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
    {{ source('br_rj_riodejaneiro_rdo', 'rdo40_tratado') }}
    
  WHERE
    data BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
         AND DATE('{{ var('data_fim_subsidio') }}') 
    AND DATE(data) NOT IN ( {{ var('datas_invalidas') }} )
  GROUP BY
    1,2
),
remun_tarifaria_e_subsidio AS (
  SELECT
      s.data,
      s.consorcio,
      s.servico,
      ROUND(sum(ifnull(remuneracao_tarifaria, 0)),2) as remuneracao_tarifaria,
      ROUND(sum(ifnull(subsidio, 0)), 2) as subsidio
  FROM
    servico_dia_valido s
  LEFT JOIN
    rdo
  USING
      (data, servico)
  GROUP BY s.data, s.consorcio, s.servico
)
SELECT *,
(ROUND(remuneracao_tarifaria + subsidio, 2)) as receita_aferida
FROM remun_tarifaria_e_subsidio