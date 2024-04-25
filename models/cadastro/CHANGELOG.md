# Changelog - cadastro

## [1.1.1] - 2024-04-25

### Modificado

- Adicionada coluna de modo na tabela `cadastro.consorcio`(https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)
- Adicionadas descrições das colunas modo e razão social no schema (https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)
- Adicionado source para `cadastro_staging.consorcio_modo` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)


## [1.1.0] - 2024-04-18

### Modificado

- Filtra dados dos modos Escolar, Táxi, TEC e Fretamento no modelo `operadoras.sql`
- Altera join da Jaé com o STU no modelo `operadoras.sql`, considerando o modo BRT como ônibus, para ser possível ligar a MobiRio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/273)

### Corrigido

- Reverte o tratamento do modelo `consorcios.sql` visto que a MobiRio está cadastrada na nova extração dos operadores no STU (https://github.com/prefeitura-rio/queries-rj-smtr/pull/273) 

## [1.0.1] - 2024-04-16

### Corrigido

- Mudança no tratamento do modelo `consorcios.sql` para que o consórcio antigo do BRT não fique relacionado à MobiRio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/272)