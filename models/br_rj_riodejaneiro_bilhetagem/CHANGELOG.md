# Changelog - bilhetagem

## [1.0.2] - 2024-04-18

### Modificado
- Filtra transações inválidas ou de teste no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/275)
  - Transações inválidas:
    - datas anteriores a 2023-07-17
  - Transações teste:
    - linhas sem ressarcimento
- Limita quantidade de ids listados no filtro da tabela de gratuidades no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/275)

## [1.0.1] - 2024-04-16

### Adicionado 
- Adicionada descrições das colunas das tabelas: `ordem_pagamento_consorcio_dia`, `ordem_pagamento_consorcio_operador_dia`, `ordem_pagamento_dia`,`transacao_riocard`
- Adicionada a descrição da coluna intervalo_integracao na tabela `integracao`

### Corrigido
- deletada a tabela ordem_pagamento do schema

## [1.0.0] - 2024-04-05

### Adicionado
- Nova view para consultar os dados staging de transações do RioCard capturados pela Jaé: `br_rj_riodejaneiro_bilhetagem_staging/staging_transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)
- Tabela tratada de transações do RioCard capturados pela Jaé: `transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)