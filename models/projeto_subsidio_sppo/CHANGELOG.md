# Changelog - projeto_subsidio_sppo

## [6.0.0] - 2024-04-17

### Adicionado

- Adiciona descrição das tabelas `subsidio_shapes_geom`, `subsidio_trips_desaninhada` e `subsidio_quadro_horario`, bem como informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Incluídas colunas `subtipo_dia`, `feed_version`, `feed_start_date` e `tipo_os` na `subsidio_data_versao_efetiva`. O modelo passa a possuir queries diferentes, caso a `run_date` seja antes ou depois do `SUBSIDIO_V6` (`2024-04-01`). Essas colunas permanecerão nulas, caso a tabela seja executada antes dessa data (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Incluída `perc_conformidade_distancia DESC` para seleção de viagem entre múltiplos trajetos a partir `SUBSIDIO_V6` (`2024-04-01`) na `viagem_completa` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- `viagem_planejada` passa a ter duas queries, caso a `run_date` seja antes ou depois do `SUBSIDIO_V6` (`2024-04-01`). A partir dessa data, o modelo passa a depender exclusivamente das tabelas de `gtfs`, descontinuando os modelos `subsidio_shapes_geom`, `subsidio_trips_desaninhada` e `subsidio_quadro_horario` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Corrigido

- A partir da `SUBSIDIO_V6` (`2024-04-01`), os dados de GPS na `aux_registros_status_trajeto` são sempre comparados com os dados de planejamento da data de operação, bem como também serão particionados na data de operação. Com isso, viagens que iniciam em um dia e encerram no outro (`overnight`) passam a ser identificadas e seus registros sempre armazenados na data de operação, independentemente de alteração de planejamento entre as datas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- A partir da `SUBSIDIO_V6` (`2024-04-01`), as potenciais viagens identificadas na `aux_viagem_inicio_fim` serão filtradas apenas para as viagens iniciadas na data de operação, de forma a não duplicar viagens em partições diferentes (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- A partir da `SUBSIDIO_V6` (`2024-04-01`), são considerados na `aux_viagem_registros` apenas os registros na data de operação
