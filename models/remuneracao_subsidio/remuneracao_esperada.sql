/*  Remuneração Esperada */ 
WITH
  parametros_subsidio AS (
  SELECT data_inicio, data_fim, irk, status
  FROM {{ source('remuneracao_subsidio', 'parametros') }} 
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
    AND valor_subsidio_pago > 0
    ),
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
      servico,
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
    km_apurada AS quilometragem, 
    parametros.irk
  FROM
    {{ source('dashboard_subsidio_sppo', 'sumario_servico_tipo_viagem_dia') }} as km_tipo_viagem

  JOIN 
    parametros_subsidio as parametros
    ON DATE(km_tipo_viagem.DATA) BETWEEN DATE(parametros.data_inicio) AND DATE(parametros.data_fim) 
    AND km_tipo_viagem.tipo_viagem = parametros.status
  WHERE
    DATA BETWEEN DATE('{{ var('data_inicio_subsidio') }}')
    AND DATE('{{ var('data_fim_subsidio') }}')
  ),
 tabela_final AS (  
   SELECT
     DISTINCT s.data,
     consorcio,
     s.servico,
     t.tipo_viagem,
     t.quilometragem,
     t.irk
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
     r.data is null
     and r.servico is null
     and data NOT IN ( {{ var('datas_invalidas') }} )
   )
   SELECT *, ROUND((irk * quilometragem), 2) AS remuneracao_esperada 
   FROM tabela_final
