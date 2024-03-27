{{ config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['shape_id', 'feed_start_date'],
    alias = 'shapes_geom'
) }} 


WITH contents AS (
    SELECT 
        shape_id,
        ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) AS ponto_shape,
        shape_pt_sequence,
        feed_start_date,
    FROM {{ref('shapes_gtfs2')}} s
    {% if is_incremental() -%}
        WHERE feed_start_date = '{{ var("data_versao_gtfs") }}'
    {%- endif %}
),
pts AS (
    SELECT *,
        MAX(shape_pt_sequence) OVER(PARTITION BY feed_start_date, shape_id) final_pt_sequence
    FROM contents c
    ORDER BY feed_start_date,
        shape_id,
        shape_pt_sequence
),
shapes AS (
    -- BUILD LINESTRINGS OVER SHAPE POINTS
    SELECT 
        shape_id,
        feed_start_date,
        ST_MAKELINE(ARRAY_AGG(ponto_shape)) AS shape
    FROM pts
    GROUP BY 1,
            2
),
boundary AS (
    -- EXTRACT START AND END POINTS FROM SHAPES
    SELECT
        c1.shape_id,
        c1.ponto_shape start_pt,
        c2.ponto_shape end_pt,
        c1.feed_start_date
    FROM 
        (
            SELECT *
            FROM pts
            WHERE shape_pt_sequence = 1
        ) c1
    JOIN 
        (
            SELECT *
            FROM pts
            WHERE shape_pt_sequence = final_pt_sequence
        ) c2 
    ON c1.shape_id = c2.shape_id
    AND c1.feed_start_date = c2.feed_start_date
),
merged AS (
    -- JOIN SHAPES AND BOUNDARY POINTS
    SELECT 
        s.*,
        b.* EXCEPT(feed_start_date, shape_id),
        ROUND(ST_LENGTH(shape), 1) shape_distance,
    FROM 
        shapes s
    JOIN 
        boundary b 
    ON 
        s.shape_id = b.shape_id
    AND 
        s.feed_start_date = b.feed_start_date
),
ids AS (
    SELECT 
        fi.feed_version,
        m.feed_start_date,
        fi.feed_end_date,
        m.shape_id,
        m.shape,
        m.shape_distance,
        m.start_pt,
        m.end_pt,
        ROW_NUMBER() OVER(PARTITION BY m.feed_start_date, m.shape_id) rn
    FROM 
        merged m
    JOIN 
        {{ ref('feed_info_gtfs2') }} fi 
    ON 
        m.feed_start_date = fi.feed_start_date
    {% if is_incremental() -%}
        WHERE fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
    {%- endif %}
)
SELECT 
    * EXCEPT(rn),
    '{{ var("version") }}' as versao_modelo
FROM ids
WHERE rn = 1