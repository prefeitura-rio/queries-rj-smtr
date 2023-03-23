# Dashboard Subsídio SPPO
* Versão: 1.0.0
* Data de início: 16/01/2023

![dashboard_subsidio_sppo](https://user-images.githubusercontent.com/66736583/227256098-6371bf20-d031-483d-8a20-f211ff552c25.png)

<div align="justify">

## Objetivo

Este modelo tem por objetivo final calcular o valor diário de subsídio para as linhas que operam o Serviço Público de Transporte de Passageiros por Meio de Ônibus do Município do Rio de Janeiro (SPPO).
  
## Dados de Entrada
- Ordem de Serviço (OS), emitida pela Secretaria Municipal de Transportes (SMTR);
- Tipos de viagem, conforme status diário de cada veículo (ver modelo [`veiculo`](https://github.com/prefeitura-rio/queries-rj-smtr/tree/master/models/veiculo));
  * `Nao licenciado` - caso a viagem tenha sido realizada por veículo não identificado como licenciado;
  * `Licenciado sem ar` - caso a viagem tenha sido realizada por veículo  licenciado com ar condicionado;
  * `Licenciado com ar e não autuado (023.II)` - caso a viagem tenha sido realizada por veículo licenciado com ar condicionado e que não tenha sido identificada nenhuma infração relativa ao código `023.II - ART 023 INC II DEC 36343/12 - Inoperância ou mau funcionamento do sistema de ar condicionado, nos veículos em que seja exigida a utilização do equipamento` na data;
  * `Licenciado com ar e autuado (023.II)` - caso a viagem tenha sido realizada por veículo licenciado com ar condicionado e que não tenha sido identificada alguma infração relativa ao código `023.II` na data.
- Viagens apuradas através de GPS (ver modelo [`projeto_subsidio_sppo`](https://github.com/prefeitura-rio/queries-rj-smtr/tree/master/models/projeto_subsidio_sppo)).

## Método

### 1. Apuração de Distância Realizada
Com base nas viagens apuradas, é realizado um agrupamento diário por serviço considerando os tipos de viagens para calcular a quantidade de viagens e a distância apurada.

A distância diária apurada considera a distância planejada multiplicada pela quantidade de viagens realizadas.

### 2. Apuração de Remuneração
Considerando o tipo de viagem e a distância apurada, é calculado o valor de remuneração conforme [DECRETO RIO N.o 51.889/2022](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2022/12/DECRETO_RIO_No_51889_DE_26_DE_DEZEMBRO_DE_2022.pdf), alterado pelo [DECRETO RIO N.o 51.915/2023](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2023/01/DECRETO-RIO-No-51915-DE-2-DE-JANEIRO-DE-2023-1.pdf) e pelo [DECRETO RIO N.o 51.940/2023](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2023/01/Decreto-51940-de-16-de-janeiro-de-2023.pdf):
| Tipo de Viagem | Valor de Remuneração (R$/km)
|------------------|---|
| `Nao licenciado` | 0.00 |
| `Licenciado sem ar` | 1.97 |
| `Licenciado com ar e não autuado (023.II)` | 2.81 |
| `Licenciado com ar e autuado (023.II)` | 0.00 |

### 3. Apuração do Percentual Diário de Operação (POD)
A distância diária apurada é relacionada com a distância diária planejada  para o serviço, sendo a razão entre esse parâmetros multiplicada por 100 é definida como Percentual Diário de Operação (POD).

Com base no POD, é definido o valor de remuneração ou penalidade para cada serviço conforme [DECRETO RIO N.o 51.889/2022](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2022/12/DECRETO_RIO_No_51889_DE_26_DE_DEZEMBRO_DE_2022.pdf), alterado pelo [DECRETO RIO N.o 51.915/2023](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2023/01/DECRETO-RIO-No-51915-DE-2-DE-JANEIRO-DE-2023-1.pdf) e pelo [DECRETO RIO N.o 51.940/2023](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2023/01/Decreto-51940-de-16-de-janeiro-de-2023.pdf):
| Intervalo do POD | Remuneração (R$) | Tipo de Penalidade  | Penalidade (R$) |
|------------------|--------------------|-------------|------------|
| [80,100] | Valor Apurado | Não há | 0.00 |
| [60,80) | 0.00 | Não há | 0.00 |
| [40,60) | 0.00 | Média | 563.28 |
| [00,40) | 0.00 | Grave | 1126.55 |
 
</div>
