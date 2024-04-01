# Changelog - veiculo

## [X.0.0] - 2024-03-30

### Adicionado

- Nova tabela de atualização do ano de vistoria dos ônibus a partir dos
  dados enviados pela
  Coordenadoria Geral de Fiscalização e Licenciamento (CGFL) em
  2024-03-20:
  `aux_sppo_licenciamento_vistoria_atualizada.sql` (#239)
  - A tabela contém todos os veículos vistoriados em 2023 e
  2024 (incluindo agendados mas ainda pendentes)

### Alterado

- Adiciona coluna `data_inicio_vinculo` na tabela `sppo_licenciamento` (#239)
- Adiciona indicador de vistoria ativa do veículo na tabela
  `sppo_veiculo_dia` para versão 5.0.0 do subsídio (#239)
- Atualiza documentação de tabelas e colunas (#239)
- Alterações feitas em #229 e #236 corrigidas em #239

## Corrigido

- Corrige versão dos dados de licenciamento do STU a partir de 01/03/24
  na tabela `sppo_licenciamento` (#239)
  - Versão dos dados foi fixada em 25/03 (última extração recebida) devido uma
    falha de atualização da fonte de dados (SIURB) aberta em 22/01 que
    ainda não foi resolvida