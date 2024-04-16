# Changelog - veiculo

## [1.1.1] - 2024-04-16

#### Corrigido

- Cria lógica de deduplicação na tabela `sppo_registro_agente_verao` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/271)


## [1.1.0] - 2024-04-15

#### Alterado

- Reorganizar modelos intermediários de licenciamento em staging (https://github.com/prefeitura-rio/queries-rj-smtr/pull/255)
- Atualiza schema para refletir as alterações (https://github.com/prefeitura-rio/queries-rj-smtr/pull/255)

## [1.0.1] - 2024-04-05

#### Alterado
- Altera a localização da verificação de validade da vistoria de `sppo_licenciamento` para `sppo_veiculo_dia` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/252)
- Adiciona coluna `data_inicio_veiculo` na tabela `sppo_licenciamento` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/252)

## [1.0.0] - 2024-03-30

### Adicionado

- Nova tabela de atualização do ano de vistoria dos ônibus a partir dos
  dados enviados pela
  Coordenadoria Geral de Fiscalização e Licenciamento (CGFL) em
  2024-03-20:
  `aux_sppo_licenciamento_vistoria_atualizada.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
  - A tabela contém todos os veículos vistoriados em 2023 e
  2024 (incluindo agendados mas ainda pendentes)

### Alterado

- Adiciona coluna `data_inicio_vinculo` na tabela `sppo_licenciamento` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Adiciona indicador de vistoria ativa do veículo na tabela
  `sppo_veiculo_dia` para versão 5.0.0 do subsídio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Atualiza documentação de tabelas e colunas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Alterações feitas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/229 e https://github.com/prefeitura-rio/queries-rj-smtr/pull/236 corrigidas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/239

## Corrigido

- Corrige versão dos dados de licenciamento do STU a partir de 01/03/24
  na tabela `sppo_licenciamento` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
  - Versão dos dados foi fixada em 25/03 (última extração recebida) devido uma
    falha de atualização da fonte de dados (SIURB) aberta em 22/01 que
    ainda não foi resolvida

