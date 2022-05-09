-- Todas as viagens classificadas
-- tipo_viagem: 
-- "Completa linha correta" => circular, nao_circular OK
-- "Completa linha incorreta" => sem priorizacao de trajeto num mesmo
-- período de tempo

-- Seleciona viagens completas de acordo com a conformidade
select 
    id_viagem,
    data,
    tipo_dia,
    consorcio,
    id_empresa,
    id_veiculo,
    servico_informado,
    servico_realizado,
    shape_id,
    sentido,
    datetime_partida,
    datetime_chegada,
    case 
        when servico_realizado = servico_informado
        then "Completa linha correta"
        else "Completa linha incorreta"
        end as tipo_viagem,
    inicio_periodo,
    fim_periodo,
    viagens_planejadas,
    tempo_planejado,
    tempo_viagem,
    distancia_teorica,
    distancia_aferida,
    n_registros_shape,
    n_registros_total,
    n_registros_minuto,
    perc_conformidade_shape,
    perc_conformidade_distancia,
    perc_conformidade_registros,
    perc_conformidade_tempo,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    {{ ref("aux_viagem_conformidade") }}
where (
    perc_conformidade_distancia >= {{ var("perc_conformidade_distancia_min") }}
    and perc_conformidade_distancia <= {{ var("perc_conformidade_distancia_max") }}
    )
and (
    perc_conformidade_shape >= {{ var("perc_conformidade_shape_min") }}
)
and (
    perc_conformidade_registros >= {{ var("perc_conformidade_registros_min") }}
)