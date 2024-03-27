{{config( 
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id','feed_start_date'],
    alias = 'trips' 
)}}


SELECT 
    fi.feed_version,
    SAFE_CAST(t.data_versao AS DATE) as feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(JSON_VALUE(t.content, '$.route_id') AS STRING) route_id,
    SAFE_CAST(JSON_VALUE(t.content, '$.service_id') AS STRING) service_id,
    SAFE_CAST(t.trip_id AS STRING) trip_id,
    SAFE_CAST(JSON_VALUE(t.content, '$.trip_headsign') AS STRING) trip_headsign,
    SAFE_CAST(JSON_VALUE(t.content, '$.trip_short_name') AS STRING) trip_short_name,
    SAFE_CAST(JSON_VALUE(t.content, '$.direction_id') AS STRING) direction_id,
    SAFE_CAST(JSON_VALUE(t.content, '$.block_id') AS STRING) block_id,
    SAFE_CAST(JSON_VALUE(t.content, '$.shape_id') AS STRING) shape_id,
    SAFE_CAST(JSON_VALUE(t.content, '$.wheelchair_accessible') AS STRING) wheelchair_accessible,
    SAFE_CAST(JSON_VALUE(t.content, '$.bikes_allowed') AS STRING) bikes_allowed,
    '{{ var("version") }}' as versao_modelo
FROM 
    {{ source(
        'br_rj_riodejaneiro_gtfs_staging',
        'trips'
    ) }} t
JOIN
    {{ ref('feed_info_gtfs2') }} fi 
ON 
    t.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
    WHERE 
        t.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}
