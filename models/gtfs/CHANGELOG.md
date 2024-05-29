# Changelog - gtfs

## [1.1.4] - 2024-05-22

### Alterado

- Alterado tratamento das colunas `inicio_periodo` e `fim_periodo`, mantendo como STRING nos modelos `ordem_servico_sentido_atualizado_aux_gtfs2.sql` e `ordem_servico_trajeto_alternativo_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Incluída coluna `tipo_os` no JOIN da CTE `ordem_servico_tratada` no modelo `ordem_servico_trips_shapes_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Incluídos serviços com quantidade de partidas nulas no modelo `ordem_servico_sentido_atualizado_aux_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Alterado tratamento da coluna `evento` no modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)

## [1.1.3] - 2024-05-21

### Corrigido
- Corrige atualização incremental da coluna `feed_end_date` nos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/321):
  - `agency_gtfs.sql`
  - `calendar_dates_gtfs.sql`
  - `fare_attributes_gtfs.sql`
  - `fare_rules_gtfs.sql`
  - `frequencies_gtfs.sql`
  - `routes_gtfs.sql`
  - `shapes_geom_gtfs.sql`
  - `shapes_gtfs.sql`
  - `stop_times_gtfs.sql`
  - `stops_gtfs.sql`
  - `trips_gtfs.sql`


## [1.1.2] - 2024-05-21

### Adicionado

- Adiciona coluna `tipo_os` no `schema.yml` em relação ao modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/320)

### Corrigido

- Corrige tratamento da coluna `tipo_os` do modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/320)

## [1.1.1] - 2024-05-21

### Adicionado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/318)

### Corrigido

- Corrige tratamento da coluna `tipo_os` do modelo `ordem_servico_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/318)

## [1.1.0] - 2024-05-13

### Alterado

- Altera nomes das tabelas do dataset `gtfs` e suas referencias em outras tabelas removendo o 2 do final e o alias

## [1.0.2] - 2024-04-29

### Corrigido

- Corrige tratamento por partição do modelo `ordem_servico_trips_shapes_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/283)

## [1.0.1] - 2024-04-19

### Alterado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_trips_shapes_gtfs2.sql` e atualiza descrição no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/278)

### Corrigido

- Corrige tratamento de colunas de tempo dos modelos `ordem_servico_sentido_atualizado_aux_gtfs2.sql` e `ordem_servico_trajeto_alternativo_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/278)

## [1.0.0] - 2024-04-18

### Adicionado

- Cria modelos:
  - `trips_filtrada_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
    - Neste modelo, é identificada uma trip de referência para cada serviço regular considerando a partição pela agregação das colunas `feed_version, trip_short_name, tipo_dia, direction_id`
    - Também são identificadas todas as trips de referência para os trajetos alternativos considerando a partição pela agregação das colunas `feed_version, trip_short_name, tipo_dia, direction_id, shape_id`
    - Em ambos os casos são ordenados por `feed_version, trip_short_name, tipo_dia, direction_id, shape_distance DESC`, privilegiando sempre a seleção dos trajetos mais longos
  - `ordem_servico_sentido_atualizado_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_gtfs2.sql` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trips_shapes_gtfs2.sql` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Adiciona descrição dos modelos `ordem_servico_trajeto_alternativo_gtfs2.sql` e `ordem_servico_trips_shapes_gtfs2.sql`, bem como informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Alterada descrição do modelo `feed_info_gtfs2.sql`, `shapes_geom_gtfs2.sql`, `ordem_servico_gtfs2.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Corrigido

- Refatora, otimiza e corrige quebra de shapes circulares no modelo `shapes_geom_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)