# Changelog - projeto_subsidio_sppo

## [7.0.3] - 2024-06-12

### Alterado

- Altera lógica do modelo `subsidio_data_versao_efetiva.sql`, de forma a considerar por regra sempre o mesmo intervalo de datas do modelo `feed_info_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/337)

## [7.0.2] - 2024-05-29

### Adicionado

- Adicionados feriado de Corpus Christi (2024-05-30) e Ponto Facultativo (2024-05-31) no modelo `subsidio_data_versao_efetiva` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/330)

## [7.0.1] - 2024-05-22

### Adicionado

- Adicionada coluna `datetime_ultima_atualizacao` no modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/324)

### Alterado

- Alterada descrição dos modelos `viagem_planejada.sql` e `viagem_completa.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/324)

### Corrigido

- Corrigido tratamento da coluna `id_tipo_trajeto` no modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/324)

## [7.0.0] - 2024-05-22

### Adicionado

- Adicionada parametrização do intervalo para dados de GPS a depender da data no modelo `aux_registros_status_trajeto.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Adicionado filtro de viagens a depender da data no modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Adicionada coluna `datetime_ultima_atualizacao` no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)

### Alterado

- Alterado datatype das colunas `inicio_periodo` e `fim_periodo` de STRING para DATETIME no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Alterado datatype das colunas `horario_inicio` e `horario_fim` de TIME para STRING no modelo `deprecated.subsidio_quadro_horario.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Alterada ordem de prioridade para seleção de viagens no modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)

### Removido

- Removido filtro de viagens realizadas na data de apuração a depender da data no modelo `aux_viagem_inicio_fim.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)

## [6.0.5] - 2024-05-08

### Removido

- Removido planejamento específico de `2024-05-04` e `2024-05-05` em razão do evento "Madonna · The Celebration Tour in Rio" (Processo Rio MTR-PRO-2024/06338) no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/300)

## [6.0.4] - 2024-05-08

### Adicionado

- Adicionado planejamento de Maio/Q1/2024 no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/299)
- Adicionado planejamento específico de `2024-05-04` e `2024-05-05` em razão do evento "Madonna · The Celebration Tour in Rio" (Processo Rio MTR-PRO-2024/06338) no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/299)

### Corrigido

- Corrigido tratamento do modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/299)

## [6.0.3] - 2024-04-29

### Corrigido

- Corrigido tratamento do modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/283)

## [6.0.2] - 2024-04-22

### Adicionado

- Adicionado planejamento de Abril/Q2/2024 no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/279)
- Adicionado `2024-04-22` como `Ponto Facultativo` em razão do [`DECRETO RIO Nº 54267/2024`](https://doweb.rio.rj.gov.br/apifront/portal/edicoes/imprimir_materia/1046645/6539) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/279)

### Corrigido

- Corrigido e refatorado o tratamento do modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/279)

## [6.0.1] - 2024-04-19

### Corrigido

- Corrige união do modelo `viagem_planejada.sql` com o modelo o `ordem_servico_trips_shapes_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/278)

## [6.0.0] - 2024-04-18

### Adicionado

- Adicionada descrição dos modelos `subsidio_shapes_geom.sql`, `subsidio_trips_desaninhada.sql` e `subsidio_quadro_horario.sql`, bem como 
informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Adicionada descrição do modelo `ssubsidio_data_versao_efetiva.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Movidos os modelos `subsidio_shapes_geom.sql`, `subsidio_trips_desaninhada.sql` e `subsidio_quadro_horario.sql` para a pasta `deprecated` em razão de terem sido descontinuados (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Incluídas as colunas `subtipo_dia`, `feed_version`, `feed_start_date` e `tipo_os` no modelo `subsidio_data_versao_efetiva.sql`. O modelo passa a possuir queries diferentes, caso a `run_date` seja antes ou depois do `SUBSIDIO_V6` (`2024-04-01`). Essas colunas permanecerão nulas, caso a tabela seja executada antes dessa data (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Alterada para `ORDER BY perc_conformidade_shape DESC, id_tipo_trajeto` para seleção de viagem entre múltiplos trajetos a partir `SUBSIDIO_V6` (`2024-04-01`) no modelo `viagem_completa.sql` de forma a privilegiar, em caso do mesmo `perc_conformidade_shape`, o trajeto regular em detrimento do alternativo (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Incluídas as colunas `id_tipo_trajeto` e `feed_version` no modelo `viagem_planejada.sql` Essas colunas permanecerão nulas, caso a tabela seja executada antes dessa data. O modelo passa a ter duas queries, caso a `run_date` seja antes ou depois do `SUBSIDIO_V6` (`2024-04-01`). A partir dessa data, o modelo passa a depender exclusivamente das tabelas de `gtfs`, descontinuando os modelos `subsidio_shapes_geom.sql`, `subsidio_trips_desaninhada.sql` e `subsidio_quadro_horario.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Alterada descrição do modelo `viagem_planejada.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Corrigido

- A partir da `SUBSIDIO_V6` (`2024-04-01`), os dados de GPS no modelo `aux_registros_status_trajeto.sql` são sempre comparados com os dados de planejamento da data de operação, bem como também serão particionados na data de operação. Com isso, viagens que iniciam em um dia e encerram no outro (`overnight`) passam a ser identificadas e seus registros sempre armazenados na data de operação, independentemente de alteração de planejamento entre as datas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- A partir da `SUBSIDIO_V6` (`2024-04-01`), as potenciais viagens identificadas no modelo `aux_viagem_inicio_fim.sql` serão filtradas apenas para as viagens iniciadas na data de operação, de forma a não duplicar viagens em partições diferentes (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- A partir da `SUBSIDIO_V6` (`2024-04-01`), são considerados no modelo `aux_viagem_registros.sql` apenas os registros na data de operação
