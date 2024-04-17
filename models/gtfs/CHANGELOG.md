# Changelog - gtfs

## [1.0.0] - 2024-04-17

### Adicionado

- Cria modelos:
  - `trips_filtrada_aux_gtfs2` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_sentido_atualizado_aux_gtfs2` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs2` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_gtfs2` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trips_shapes_gtfs2` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Adiciona descrição das tabelas `ordem_servico_trajeto_alternativo_gtfs2` e `ordem_servico_trips_shapes_gtfs2`, bem como informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Adiciona coluna `tipo_os` na tabela `ordem_servico_gtfs2` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Refatora e otimiza modelo `shapes_geom_gtfs2` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)