/* Remuneração esperada*/ 

WITH
  parametros AS (
  SELECT DISTINCT data_inicio, data_fim, irk, status
  FROM {{ source('dashboard_subsidio_sppo', 'subsidio_parametros') }}  
  ),
  subsidiado AS (
  SELECT
    DISTINCT DATA,
    servico
  FROM
    {{ source('dashboard_subsidio_sppo', 'sumario_servico_dia_historico') }} 
  WHERE
    DATA BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
    AND DATE('{{ var('data_fim_subsidio') }}')
    AND valor_subsidio_pago > 0),
  recurso AS (
  SELECT
    DISTINCT data_viagem AS DATA,
    servico
  FROM
     {{ source('recurso_subsidio_sppo', 'reprocessamento') }} 
  WHERE
    data_viagem BETWEEN DATE('{{ var('data_inicio_subsidio') }}') 
    AND DATE('{{ var('data_fim_subsidio') }}')
  UNION ALL (
    SELECT
      DISTINCT data_viagem AS DATA,
      servico
    FROM
      {{ source('recurso_subsidio_sppo', 'bloqueio_via') }}
    WHERE
      data_viagem BETWEEN DATE('{{ var('data_inicio_subsidio') }}')
      AND DATE('{{ var('data_fim_subsidio') }}') 
    )
  ),
  km_tipo_viagem AS (
  SELECT
    DISTINCT DATA,
    consorcio,
    servico,
    tipo_viagem,
    km_apurada AS quilometragem
  FROM
    {{ source('remuneracao_subsidio', 'sumario_servico_tipo_viagem_dia') }} as k
  WHERE
    DATA BETWEEN DATE('{{ var('data_inicio_subsidio') }}')
    AND DATE('{{ var('data_fim_subsidio') }}')
    ),
  todas_viagens AS (
  SELECT
    DISTINCT s.data,
    consorcio,
    s.servico,
    t.tipo_viagem,
    t.quilometragem
  FROM
    subsidiado AS s
  LEFT JOIN
    km_tipo_viagem AS t
  USING
    (DATA,
      servico)
  LEFT JOIN
    recurso AS r
  USING
    (DATA,
      servico)
  WHERE
    r.data IS NULL
    AND r.servico IS NULL
    AND DATA NOT IN ( {{ var('datas_invalidas') }} )
      ) 
    SELECT todas_viagens.*, parametros.irk, ROUND((parametros.irk * todas_viagens.quilometragem), 2) AS remuneracao_esperada 
    FROM todas_viagens 
    LEFT JOIN parametros
    ON todas_viagens.DATA BETWEEN parametros.data_inicio AND parametros.data_fim
    AND todas_viagens.tipo_viagem = parametros.status
