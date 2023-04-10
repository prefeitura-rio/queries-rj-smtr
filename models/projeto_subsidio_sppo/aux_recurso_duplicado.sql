{{ 
config(
    materialized='ephemeral'
)
}}

with recursos as (
    select *
    from {{ ref("aux_recurso_fora_prazo") }}
    where id_julgamento is null
),
recursos_duplicados as (
    SELECT
        s1.protocolo,
        3 as id_julgamento,
        concat("Pedidos sobrepostos: ", string_agg(s2.protocolo, ", ")) as observacao
    FROM recursos s1
    inner join recursos s2
    on 
        s1.id_veiculo = s2.id_veiculo
        and s1.datetime_partida <= s2.datetime_partida
        and s2.datetime_partida < s1.datetime_chegada
        and s1.protocolo != s2.protocolo
    group by 1,2
)
select
    r.* except(id_julgamento, observacao),
    coalesce(r.id_julgamento, rd.id_julgamento) as id_julgamento,
    coalesce(r.observacao, rd.observacao) as observacao
from {{ ref("aux_recurso_fora_prazo") }} r
left join recursos_duplicados rd
on r.protocolo = rd.protocolo