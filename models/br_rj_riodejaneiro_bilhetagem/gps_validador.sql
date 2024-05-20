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
    id_servico_jae,
    -- s.servico,
    l.nr_linha AS servico_jae,
    l.nm_linha AS descricao_servico_jae,
    CASE
      WHEN modo = "VLT" THEN SUBSTRING(id_veiculo, 1, 3)
      WHEN modo = "BRT" THEN NULL
      ELSE id_veiculo
    END AS id_veiculo,
    id_validador,
    id_transmissao_gps,
    latitude,
    longitude,
    sentido,
    estado_equipamento,
    temperatura,
    '{{ var("version") }}' as versao
FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id_transmissao_gps ORDER BY datetime_captura DESC) AS rn
    FROM
        {{ ref("gps_validador_aux") }}
    
    {% if is_incremental() %}
      WHERE
          DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
          AND datetime_captura > DATETIME("{{var('date_range_start')}}") AND datetime_captura <= DATETIME("{{var('date_range_end')}}")
    {% endif %}
)
WHERE
    rn = 1
    AND modo != "Van"
