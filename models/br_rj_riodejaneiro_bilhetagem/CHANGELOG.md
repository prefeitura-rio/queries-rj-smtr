# Changelog - bilhetagem

## [1.0.2] - 2024-04-17

### Corrigido
- Filtra transações inválidas ou de teste no modelo `transacao.sql`
  - Transações inválidas:
    - datas anteriores a 2023-07-17
  - Transações teste:
    - linhas sem ressarcimento 

## [1.0.0] - 2024-04-05

### Adicionado
- Nova view para consultar os dados staging de transações do RioCard capturados pela Jaé: `br_rj_riodejaneiro_bilhetagem_staging/staging_transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)
- Tabela tratada de transações do RioCard capturados pela Jaé: `transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)