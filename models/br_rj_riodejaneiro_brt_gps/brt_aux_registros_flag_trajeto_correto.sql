{{
  config(
    materialized='ephemeral'
  )
}}
/*
Descrição:
Calcula se o veículo está dentro do trajeto correto dado o traçado (shape) cadastrado no SIGMOB em relação à linha que está sendo
transmitida.
1. Calcula as intersecções definindo um 'buffer', utilizado por st_dwithin para identificar se o ponto está à uma
distância menor ou igual ao tamanho do buffer em relação ao traçado definido no SIGMOB.
2. Calcula um histórico de intersecções nos ultimos 10 minutos de registros de cada carro. Definimos que o carro é
considerado fora do trajeto definido se a cada 10 minutos, ele não esteve dentro do traçado planejado pelo menos uma
vez.
3. Identifica se a linha informada no registro capturado existe nas definições presentes no SIGMOB.
4. Definimos em outra tabela uma 'data_versao_efetiva', esse passo serve tanto para definir qual versão do SIGMOB utilizaremos em 
caso de falha na captura, quanto para definir qual versão será utilizada para o cálculo retroativo do histórico de registros que temos.
5. Como não conseguimos identificar o itinerário que o carro está realizando, no passo counts, os resultados de
intersecções são dobrados, devido ao fato de cada linha apresentar dois itinerários possíveis (ida/volta). Portanto,
ao final, realizamos uma agregação LOGICAL_OR que é true caso o carro esteja dentro do traçado de algum dos itinerários
possíveis para a linha informada.
*/
WITH
  registros AS (
    SELECT id_veiculo, servico as linha, latitude, longitude, data, posicao_veiculo_geo, timestamp_gps
    FROM
      {{ ref('brt_aux_registros_filtrada') }} r
    {% if not flags.FULL_REFRESH -%}
    WHERE
    data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
    AND timestamp_gps > "{{var('date_range_start')}}" and timestamp_gps <="{{var('date_range_end')}}"
    {%- endif %}
  ),
  intersec AS (
    SELECT
      r.*,
      s.data_versao,
      s.linha_gtfs,
      s.route_id,
      -- 1. Buffer e intersecções
      CASE
        WHEN st_dwithin(shape, posicao_veiculo_geo, {{ var('tamanho_buffer_metros') }}) THEN TRUE
        ELSE FALSE
      END AS flag_trajeto_correto,
      -- 2. Histórico de intersecções nos últimos 10 minutos a partir da timestamp_gps atual
      CASE
        WHEN 
          COUNT(CASE WHEN st_dwithin(shape, posicao_veiculo_geo, {{ var('tamanho_buffer_metros') }}) THEN 1 END) 
          OVER (PARTITION BY id_veiculo 
                ORDER BY UNIX_SECONDS(TIMESTAMP(timestamp_gps)) 
                RANGE BETWEEN {{ var('intervalo_max_desvio_segundos') }} PRECEDING AND CURRENT ROW) >= 1 
          THEN True
        ELSE False
      END AS flag_trajeto_correto_hist,
      -- 3. Identificação de cadastro da linha no SIGMOB
      CASE WHEN s.linha_gtfs IS NULL THEN False ELSE True END AS flag_linha_existe_sigmob,
    -- 4. Join com data_versao_efetiva para definição de quais shapes serão considerados no cálculo das flags
    FROM registros r
    LEFT JOIN (
      SELECT * 
      FROM {{ ref('shapes_geom') }}
      WHERE id_modal_smtr in ({{ var('brt_id_modal_smtr')|join(', ') }})
      AND data_versao = "{{var('versao_fixa_sigmob')}}"
    ) s
    ON
      r.linha = s.linha_gtfs
  )
    -- 5. Agregação com LOGICAL_OR para evitar duplicação de registros
    SELECT
      id_veiculo,
      linha as servico,
      linha_gtfs,
      route_id,
      data,
      timestamp_gps,
      LOGICAL_OR(flag_trajeto_correto) AS flag_trajeto_correto,
      LOGICAL_OR(flag_trajeto_correto_hist) AS flag_trajeto_correto_hist,
      LOGICAL_OR(flag_linha_existe_sigmob) AS flag_linha_existe_sigmob,
      -- STRUCT({{ maestro_sha }} AS versao_maestro, 
      --       {{ maestro_bq_sha }} AS versao_maestro_bq,
      --       data_versao AS data_versao_sigmob
      --       ) versao
    FROM
      intersec i
    GROUP BY
      id_veiculo,
      linha,
      linha_gtfs,
      route_id,
      data,
      data_versao,
      timestamp_gps
  