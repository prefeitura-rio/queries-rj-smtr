{{
    config(
        materialized='table'
    )
}}

SELECT
    g.id_servico AS id_servico_gtfs,
    j.cd_linha AS id_servico_jae,
    COALESCE(g.servico, j.nr_linha) AS servico,
    g.servico AS servico_gtfs,
    j.nr_linha AS servico_jae,
    COALESCE(g.descricao_servico, j.nm_linha) AS descricao_servico,
    g.descricao_servico AS descricao_servico_gtfs,
    j.nm_linha AS descricao_servico_jae,
    g.latitude,
    g.longitude,
    g.tabela_origem_gtfs,
    g.feed_start_date AS feed_start_date_gtfs,
    '{{ var("version") }}' as versao
FROM
    {{ ref("staging_linha") }} j
FULL OUTER JOIN
    {{ ref("servicos_gtfs_aux") }} g
ON 
    COALESCE(j.gtfs_route_id, j.gtfs_stop_id) = g.id_servico