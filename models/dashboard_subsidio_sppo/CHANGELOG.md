# Changelog - dashboard_subsidio_sppo

## [5.0.0] - 2024-03-30

### Adicionado

- Nova tabela de referência do valor do subsídio por tipo de viagem,
  inclusão do novo tipo `"Não vistoriado"`:
  `subsidio_valor_km_tipo_viagem.sql` (#239)

### Alterado

- Refatora tabelas `viagens_remuneradas.sql` e
  `sumario_servico_dia_tipo_sem_glosa.sql` para uso da nova tabela de
  referência (#239)
- Adiciona novo tipo de viagem/status `"Não vistoriado"` nas tabelas
  `sumario_servico_dia_tipo.sql`,
  `sumario_servico_dia_tipo_sem_glosa.sql`,
  `sumario_servico_tipo_viagem_dia.sql` (#239)
- Simplifica padronização dos tipos de viagens nas tabelas
  `sumario_servico_dia_tipo.sql` e `sumario_servico_tipo_viagem_dia.sql` (#239)
- Atualiza documentação de tabelas e colunas (#239)
- Alterações feitas em #229 e #236 corrigidas em #239