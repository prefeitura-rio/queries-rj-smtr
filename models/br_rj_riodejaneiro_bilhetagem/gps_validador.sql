{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
  )
}}

SELECT
    modo,
    EXTRACT(DATE FROM datetime_gps) AS data,
    EXTRACT(HOUR FROM datetime_gps) AS hora,
    datetime_gps,
    datetime_captura,
    id_operadora,
    operadora,
    servico,
    id_veiculo,
    id_validador,
    id_transmissao_gps,
    latitude,
    longitude,
    sentido,
    estado_equipamento,
    temperatura,
    '{{ var("version") }}' as versao
FROM
    {{ ref("gps_validador_aux") }}
WHERE
    {% if is_incremental() %}
        DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
        AND datetime_captura > DATETIME("{{var('date_range_start')}}") AND datetime_captura <= DATETIME("{{var('date_range_end')}}")
        AND
    {%- endif %}
    modo != "Van"