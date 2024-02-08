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


WITH gps_deduplicado AS (
    SELECT 
        * EXCEPT(rn)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            {{ ref("staging_gps_validador") }}
        {% if is_incremental() -%}
        WHERE
            DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
            AND timestamp_captura > DATETIME("{{var('date_range_start')}}") AND timestamp_captura <= DATETIME("{{var('date_range_end')}}")
        {%- endif %}
    )
    WHERE
        rn = 1
)
SELECT
    do.modo,
    EXTRACT(DATE FROM g.data_tracking) AS data,
    EXTRACT(HOUR FROM g.data_tracking) AS hora,
    g.data_tracking AS datetime_gps,
    g.timestamp_captura AS datetime_captura,
    do.id_operadora,
    do.operadora,
    l.nr_linha AS servico,
    NULL AS id_veiculo,
    g.numero_serie_equipamento AS id_validador,
    g.id AS id_transmissao_gps,
    g.latitude_equipamento AS latitude,
    g.longitude_equipamento AS longitude,
    INITCAP(g.sentido_linha) AS sentido,
    g.estado_equipamento,
    g.temperatura,
    '{{ var("version") }}' as versao
FROM
    gps_deduplicado g
LEFT JOIN
    {{ ref("operadoras") }} AS do
ON
    g.codigo_operadora = do.id_operadora_jae
LEFT JOIN
    {{ ref("staging_linha") }} AS l
ON
    g.codigo_linha_veiculo = l.cd_linha 
    AND g.data_tracking >= l.datetime_inclusao
    
