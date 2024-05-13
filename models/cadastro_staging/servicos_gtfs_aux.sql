{{
    config(
        materialized="incremental",
        unique_key="id_servico"
    )
}}

{% set gtfs_partiton_table = ref('feed_info_gtfs') %}
{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_partition_query %}
            SELECT DISTINCT
                MAX(feed_start_date) AS feed_start_date
            FROM
                {{ gtfs_partiton_table }}
        {% endset %}

        {% set gtfs_last_partition = run_query(gtfs_partition_query).columns[0].values()[0] %}
    {% endif %}
{% endif %}


WITH routes_gtfs AS (
    SELECT
        *
    FROM
    (
        SELECT
            route_id AS id_servico,
            route_short_name AS servico,
            route_long_name AS descricao_servico,
            NULL AS latitude,
            NULL AS longitude,
            feed_start_date,
            'routes' AS tabela_origem_gtfs,
            '{{ var("version") }}' as versao,
            ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY feed_start_date DESC) AS rn
        FROM
            {{ ref("routes_gtfs") }}
        {% if is_incremental() %}
            WHERE
                feed_start_date = '{{ gtfs_last_partition }}'
        {% endif %}
    )
    WHERE rn = 1
),
stops_gtfs AS (
    SELECT
        * EXCEPT(rn)
    FROM
    (
        SELECT
            stop_id AS id_servico,
            stop_code AS servico,
            stop_name AS descricao_servico,
            stop_lat AS latitude,
            stop_lon AS longitude,
            feed_start_date,
            'stops' AS tabela_origem_gtfs,
            '{{ var("version") }}' as versao,
            ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY feed_start_date DESC) AS rn
        FROM
            {{ ref("stops_gtfs") }}
        WHERE
            location_type = '1'
            {% if is_incremental() %}
                AND feed_start_date = '{{ gtfs_last_partition }}'
            {% endif %}
    )
    WHERE rn = 1
)
SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    feed_start_date,
    tabela_origem_gtfs,
    versao
FROM
    routes_gtfs

UNION ALL

SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    feed_start_date,
    tabela_origem_gtfs,
    versao
FROM
    stops_gtfs

