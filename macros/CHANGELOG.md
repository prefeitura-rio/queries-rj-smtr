# Changelog - macros

## [1.0.0] - 2024-04-25

### Adicionado

- Criada macro `get_license_date.sql` para retornar relacionamento entre `run_date` e data de licenciamento dos modelos `sppo_licenciamento_stu_staging.sql`, `sppo_licenciamento.sql` e `sppo_veiculo_dia.sql`. Nesta macro, serão admitidas apenas versões do STU igual ou após 2024-04-09 a partir de abril/24 devido à falha de atualização na fonte da dados (SIURB) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/280)