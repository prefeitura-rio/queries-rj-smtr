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
    EXTRACT(DATE FROM g.data_tracking) AS data,
    EXTRACT(HOUR FROM g.data_tracking) AS hora,
    g.data_tracking AS datetime_recebimento_transmissao,
    g.timestamp_captura AS datetime_captura,
    do.id_operadora,
    do.operadora,
    l.nr_linha AS servico,
    g.numero_serie_equipamento AS numero_serie_validador,
    g.id AS id_transmissao_gps,
    g.latitude_equipamento AS latitude,
    g.longitude_equipamento AS longitude,
    g.prefixo_veiculo,
    g.sentido_linha,
    g.estado_equipamento,
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
    