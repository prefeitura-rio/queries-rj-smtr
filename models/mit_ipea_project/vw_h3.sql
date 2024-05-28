-- H3 Resolution 8 view, convert geometry column to GEOGRAPHY type
SELECT  tile_id,
        resolution,
        parent_id,
        ST_GEOGFROMTEXT(geometry) AS geometry, -- Convert string in GEOGRAPHY value
        ST_CENTROID(ST_GEOGFROMTEXT(geometry)) AS centroid
FROM `rj-smtr.br_rj_riodejaneiro_geo.h3_res8`