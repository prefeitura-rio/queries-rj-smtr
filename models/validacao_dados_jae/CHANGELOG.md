# Changelog - validacao_dados_jae

## [1.0.2] - 2024-05-22

### Corrigido
- Corrige variável de data no modelo `ordem_pagamento_consorcio_dia_validacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/323)
- Corrige referências nos modelos `ordem_pagamento_consorcio_dia_validacao.sql` e `ordem_pagamento_validacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/323)

## [1.0.1] - 2024-05-20

### Alterado
- Adiciona colunas `servico_jae` e `descricao_servico_jae` do modelo `transacao_invalida.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311)
- Altera nome da coluna `indicador_geolocalizacao_fora_rj` para `indicador_geolocalizacao_fora_rio` no modelo `transacao_invalida.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311)

### Corrigido
- Remove comentários do modelo `ordem_pagamento_validacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311)
- Corrige nome da coluna `indicador_intervalo_transacao_suspeito` no filtro final do modelo `integracao_invalida.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311)

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