# Changelog - gtfs

## [1.0.0] - 2024-04-17

### Adicionado

- Cria modelos:
  - `trips_filtrada_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_sentido_atualizado_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_gtfs2.sql` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trips_shapes_gtfs2.sql` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Adiciona descrição dos modelos `ordem_servico_trajeto_alternativo_gtfs2.sql` e `ordem_servico_trips_shapes_gtfs2.sql`, bem como informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Refatora e otimiza modelo `shapes_geom_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)