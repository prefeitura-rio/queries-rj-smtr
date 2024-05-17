# Changelog - validacao_dados_jae

## [1.0.1] - 2024-05-17

### Alterado
- Muda tratamento do modelo `transacao_invalida.sql` para pegar a coluna `servico` da tabela `transacao` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311)

 ### Corrigido
- Remove comentários do modelo `ordem_pagamento_validacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311)

## [1.0.0] - 2024-05-16

### Adicionado
- Cria modelos para validação dos dados da Jaé (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297):
  - **validacao_dados_jae_staging**:
    - `ordem_pagamento_consorcio_dia_validacao.sql`
    - `ordem_pagamento_consorcio_operador_dia_validacao.sql`
    - `ordem_pagamento_servico_operador_dia_validacao.sql`
  - **validacao_dados_jae**:
    - `integracao_invalida.sql`
    - `ordem_pagamento_validacao.sql`
    - `transacao_invalida.sql`
    - `veiculo_validacao.sql`