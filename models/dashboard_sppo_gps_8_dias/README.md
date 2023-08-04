# Dados para o dashboard "GPS dos ônibus"
* Versão: 0.1
* Data de início: 13/07/2023


## 1. Resumo

Este modelo tem por objetivo tratar os dados de GPS para que eles sejam exibidos no dashboard de "[GPS dos ônibus](https://app.powerbi.com/view?r=eyJrIjoiZTljNzIxNTAtN2QxZS00OTczLThjMjUtNWY1NjdjZWVlODZmIiwidCI6IjkwNzI2YWVlLWQwMmMtNDlmZS05ODlmLTQ1ZGVmM2QwNjlkYyJ9)".


## 2. Etapas

### 2.1 Importação
O modelo utilizou dados das seguintes tabelas:

rj-smtr.dashboards.gps_sppo_8_dias

rj-smtr.projeto_subsidio_sppo.viagem_planejada

### 2.2 Transformações
Os dados da tabela gps_sppo_8_dias foram filtrados de forma que o resultado final incluisse apenas aqueles serviços presentes na tabela viagem_planejada.
