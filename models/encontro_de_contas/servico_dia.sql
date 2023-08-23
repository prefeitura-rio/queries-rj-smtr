-- Serviço dia
WITH 
  -- 1. Recupera serviços-dias subsidiados
  servico_dia AS (
  SELECT DISTINCT
    data,
    consorcio,
    servico,
    viagens,
    valor_subsidio_pago AS subsidio  
  FROM
    {{ source('dashboard_subsidio_sppo', 'sumario_servico_dia_historico') }} 
  WHERE
    data BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
    AND DATE('{{ var('data_fim_subsidio') }}')
    AND data NOT IN ({{ var('datas_invalidas') }})
    AND valor_subsidio_pago > 0),
    
-- 2. Remove serviços-dia pagos por recurso
recurso AS (
    SELECT DISTINCT
       data_viagem AS data,
       servico,
    FROM
      {{ source('recurso_subsidio_sppo', 'reprocessamento') }} 

    WHERE
      data_viagem BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
    AND DATE('{{ var('data_fim_subsidio') }}')
    UNION ALL (
    SELECT DISTINCT
       data_viagem AS data,
       servico
    FROM
      {{ source('recurso_subsidio_sppo', 'bloqueio_via') }}
    WHERE
      data_viagem BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
    AND DATE('{{ var('data_fim_subsidio') }}')
    AND data_viagem NOT IN ({{ var('datas_invalidas') }})     
    )
)
-- retorna os serviços-dia válidos
    SELECT
        s.*
    FROM
        servico_dia s
    LEFT JOIN 
        recurso r
    USING
        (data, servico)
    WHERE r.data IS NULL