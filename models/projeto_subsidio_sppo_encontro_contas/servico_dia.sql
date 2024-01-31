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
  -- 1. Recupera serviços-dias subsidiados
  servico_dia AS (
  SELECT DISTINCT
    data,
    consorcio,
    servico,
    viagens,
    valor_subsidio_pago AS subsidio  
  FROM
    {{ ref("sumario_servico_dia_historico") }} -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
  WHERE
    data BETWEEN DATE('2022-06-01') 
    AND DATE('2023-11-30')
    AND data NOT IN ('2022-10-02', '2022-10-30', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')
    AND valor_subsidio_pago > 0),
    
-- 2. Remove serviços-dia pagos por recurso
recurso AS (
    SELECT DISTINCT
       data_viagem AS data,
       servico,
    FROM
      {{ source("projeto_subsidio_sppo_encontro_contas", "recursos_sppo_reprocessamento") }}
    WHERE
      data_viagem BETWEEN DATE('2022-06-01') 
    AND DATE('2023-11-30')
    UNION ALL (
    SELECT DISTINCT
       data_viagem AS data,
       servico
    FROM
      {{ source("projeto_subsidio_sppo_encontro_contas", "recursos_sppo_bloqueio_via") }}
    WHERE
      data_viagem BETWEEN DATE('2022-06-01') 
    AND DATE('2023-11-30')
    AND data_viagem NOT IN ('2022-10-02', '2022-10-30', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')     
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