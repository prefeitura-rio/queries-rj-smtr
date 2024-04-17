-- 1. Seleciona sinais de GPS registrados no período
with gps as (
    select 
        g.* except(longitude, latitude, servico),
        {% if var("run_date") > "2023-01-16" and var("run_date") < "2023-12-02" %}
        -- Substitui servicos noturnos por regulares, salvo exceções
        case
            when servico like "SN%" and servico not in ("SN006", "SN415", "SN474", "SN483")
            then REGEXP_EXTRACT(servico, r'[0-9]+')
            else servico
        end as servico,
        {% else %}
        servico,
        {% endif %}
        substr(id_veiculo, 2, 3) as id_empresa,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
        date_sub(date("{{ var("run_date") }}"), interval 1 day) as data_operacao
        {% endif %}
    from 
        `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g -- {{ ref('gps_sppo') }} g
    where (
        data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
    )
    -- Limita range de busca do gps de D-2 às 00h até D-1 às 3h
    and (
        timestamp_gps between datetime_sub(datetime_trunc("{{ var("run_date") }}", day), interval 1 day)
        and datetime_add(datetime_trunc("{{ var("run_date") }}", day), interval 3 hour)
    )
    and status != "Parado garagem"
),
-- 2. Classifica a posição do veículo em todos os shapes possíveis de
--    serviços de uma mesma empresa
status_viagem as (
    select
        {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
        data_operacao as data,
        {% else %}
        g.data,
        {% endif %}
        g.id_veiculo,
        g.id_empresa,
        g.timestamp_gps,
        timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
        g.posicao_veiculo_geo,
        TRIM(g.servico, " ") as servico_informado,
        s.servico as servico_realizado,
        s.shape_id,
        s.sentido_shape,
        s.shape_id_planejado,
        s.trip_id,
        s.trip_id_planejado,
        s.sentido,
        s.start_pt,
        s.end_pt,
        s.distancia_planejada,
        ifnull(g.distancia,0) as distancia,
        case
            when ST_DWITHIN(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }})
            then 'start'
            when ST_DWITHIN(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }})
            then 'end'
            when ST_DWITHIN(g.posicao_veiculo_geo, shape, {{ var("buffer") }})
            then 'middle'
        else 'out'
        end status_viagem
    from 
        gps g
    inner join (
        select 
            *
        from
            {{ ref("viagem_planejada") }}
        where
            {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
            data = date_sub(date("{{ var("run_date") }}"), interval 1 day)
            {% else %}
            data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}")
            {% endif %}
    ) s
    on 
        {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
        g.data_operacao = s.data
        {% else %}
        g.data = s.data
        {% endif %}
        and g.servico = s.servico
)
select 
    *,
    '{{ var("version") }}' as versao_modelo
from 
    status_viagem
