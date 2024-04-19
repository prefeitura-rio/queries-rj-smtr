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
        ST_MAKELINE(ARRAY_AGG(ponto_shape)) AS shape,
        ARRAY_AGG(ponto_shape)[ORDINAL(1)] AS start_pt,
        ARRAY_AGG(ponto_shape)[ORDINAL(ARRAY_LENGTH(ARRAY_AGG(ponto_shape)))] AS end_pt,
    FROM pts
    GROUP BY 1,
            2
),
shapes_half AS (
    -- BUILD HALF LINESTRINGS OVER SHAPE POINTS
    (
        SELECT 
            shape_id,
            feed_start_date,
            shape_id || "_0" AS new_shape_id,
            ST_MAKELINE(ARRAY_AGG(ponto_shape)) AS shape,
            ARRAY_AGG(ponto_shape)[ORDINAL(1)] AS start_pt,
            ARRAY_AGG(ponto_shape)[ORDINAL(ARRAY_LENGTH(ARRAY_AGG(ponto_shape)))] AS end_pt,
        FROM 
            pts
        WHERE 
            shape_pt_sequence <= ROUND(final_pt_sequence / 2)
        GROUP BY 
            1,
            2
    )
    UNION ALL
    (
        SELECT 
            shape_id,
            feed_start_date,
            shape_id || "_1" AS new_shape_id,
            ST_MAKELINE(ARRAY_AGG(ponto_shape)) AS shape,
            ARRAY_AGG(ponto_shape)[ORDINAL(1)] AS start_pt,
            ARRAY_AGG(ponto_shape)[ORDINAL(ARRAY_LENGTH(ARRAY_AGG(ponto_shape)))] AS end_pt,
        FROM 
            pts
        WHERE 
            shape_pt_sequence > ROUND(final_pt_sequence / 2)
        GROUP BY 
            1,
            2
    )
),
ids AS (
    SELECT
      * EXCEPT(rn)
    FROM
    (
        SELECT 
            feed_start_date,
            shape_id,
            shape,
            start_pt,
            end_pt,
            ROW_NUMBER() OVER(PARTITION BY feed_start_date, shape_id) rn
        FROM 
            shapes
    )
    WHERE rn = 1
),
union_shapes AS (
  (
    SELECT
        feed_start_date,
        shape_id,
        shape,
        start_pt,
        end_pt,
    FROM
        ids
  )
  UNION ALL
  (
    SELECT
        feed_start_date,
        new_shape_id AS shape_id,
        s.shape,
        s.start_pt,
        s.end_pt,
    FROM
        ids AS i
    LEFT JOIN
        shapes_half AS s
    USING
        (feed_start_date, shape_id)
    WHERE
        ROUND(ST_Y(i.start_pt),4) = ROUND(ST_Y(i.end_pt),4)
        AND ROUND(ST_X(i.start_pt),4) = ROUND(ST_X(i.end_pt),4)
  )
)
SELECT 
    feed_version,
    feed_start_date,
    feed_end_date,
    shape_id,
    shape,
    ROUND(ST_LENGTH(shape), 1) shape_distance,
    start_pt,
    end_pt,
    '{{ var("version") }}' as versao_modelo
FROM union_shapes AS m
LEFT JOIN 
    {{ ref('feed_info_gtfs2') }} AS fi 
USING
    (feed_start_date)
{% if is_incremental() -%}
WHERE
    fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}