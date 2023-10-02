# Georreferenciamento de dados do TG
* Versão: 0.1
* Data de início: 03/07/2023


## 1. Resumo
Este modelo tem por objetivo georreferenciar os microdados das transações do TG das empresas do Serviço Público de Transporte de Passageiros por Meio de Ônibus do Município do Rio de Janeiro (SPPO).


## 2. Etapas

### 2.1 Selecionar dados de origem

O modelo utilizou dados das seguintes tabelas:

rj-smtr.br_rj_riodejaneiro_onibus_tg.transacao

rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo

### 2.2 Transformações nos dados do TG

Foram removidas aquelas transações considerada outliers, ou seja, em que ocorreram mais de 4 transações no mesmo cartão por minuto, dada a improbabilidade de isto ocorrer no mundo real.

### 2.3 Transformações nos dados de GPS

Quanto um veículo tiver mais de uma localização dentro do mesmo minuto, será calculada a média entre as localizações (colunas latitude e longitude). Além disso, os valores destas colunas foram arredondados para três casas decimais.

### 3 Georreferenciamento das transações

O cruzamento dos dados manteve todas as observações da tabela com dados do TG e adicionou os dados de posição de acordo com as colunas id_veiculo e data_hora_arredondada.

Em seguida foram removidas as transações que não foram georreferenciadas na etapa acima.
