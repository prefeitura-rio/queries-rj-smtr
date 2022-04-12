with agency as (
    select data_versao, agency_name as consorcio, route_short_name as servico, 
    from `rj-smtr.br_rj_riodejaneiro_sigmob.routes_desaninhada`
    where idModalSmtr in ("22", "O")
    and data_versao between date("2022-03-20") and date("2022-03-27")
),
viagens as (
    select 
        a.consorcio,
        v.*,
        SUBSTR(v.shape_id, 11, 1) as sentido,
    CASE
        WHEN extract(dayofweek from datetime_partida) = 1 THEN 'Domingo'
        WHEN extract(dayofweek from datetime_partida) = 7 THEN 'Sabado'
        -- WHEN data = data_feriado THEN 'Feriado'
        ELSE 'Dia Útil'
    END tipo_dia
    from `rj-smtr-dev.projeto_subsidio_sppo_v2.viagens_validas` v
    left join agency a 
    on v.servico = a.servico
    and extract(date from datetime_partida) = a.data_versao
)
SELECT distinct
    v.* except(perc_conformidade),
    v.perc_conformidade as perc_conformidade_shape,
    100 * (v.distancia_km/v.distancia_teorica) as perc_conformidade_distancia,
    p.start_time as inicio_periodo,
    p.end_time as fim_periodo,
    p.viagens as viagens_teorico,
    p.intervalo as intervalo_teorico
from viagens v
left join `rj-smtr-dev.projeto_subsidio_sppo_v2.quadro_horario_completo` p
on v.servico = p.servico
and v.tipo_dia = p.tipo_dia
and v.sentido = p.sentido
where ((
    start_time < end_time and -- 05:00:00 as 23:00:00
    extract (time from datetime_partida) >= start_time and extract (time from datetime_partida) < end_time
) or
 ( -- 23:00:00 as 5:00:00
    start_time > end_time and ( 
        (extract (time from datetime_partida) >= start_time) -- 23:10:00
        or
        (extract (time from datetime_partida) <= end_time) -- 4:50:00
    )
))