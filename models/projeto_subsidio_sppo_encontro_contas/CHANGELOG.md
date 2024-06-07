# Changelog - projeto_subsidio_sppo_encontro_contas

## [1.0.3] - 2024-06-07

### Adicionado

- Adiciona modelo `staging.rdo_correcao_rioonibus_servico_quinzena.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)
- Adiciona modos de execução `""` (antes das alterações do Grupo de Trabalho) e `"_pos_gt"` (após as alterações do Grupo de Trabalho) conforme Processo.Rio MTR-PRO-2024/06270 e as respectivas alterações dos nomes das tabelas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)
- Adiciona novo tratamento de serviços do RDO no modelo `balanco_servico_dia_pos_gt.sql` com base no modelo `balanco_servico_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)

## [1.0.2] - 2024-05-21

### Alterado

- Adiciona refs e sources de modelos do dbt (https://github.com/prefeitura-rio/queries-rj-smtr/pull/319)

## [1.0.1] - 2024-05-16

### Alterado

- Refatora nome de colunas e adiciona schema.yml (https://github.com/prefeitura-rio/queries-rj-smtr/pull/306)

## [1.0.0] - 2024-05-14

### Adicionado

- Adiciona tabela de cálculo do encontro de contas por dia e serviço + agregações (https://github.com/prefeitura-rio/queries-rj-smtr/pull/304)

### Removido
- Versões de teste: https://github.com/prefeitura-rio/queries-rj-smtr/pull/234, https://github.com/prefeitura-rio/queries-rj-smtr/pull/233
