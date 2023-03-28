# Dashboard Subsídio SPPO
* Versão: 2.0.0
* Data de início: 16/01/2023

![dashboard_subsidio_sppo](https://user-images.githubusercontent.com/66736583/227256098-6371bf20-d031-483d-8a20-f211ff552c25.png)

<div align="justify">

# Índice
1. [Resumo](#1-resumo)
2. [Etapas](#2-etapas)
3. [Glossário](#3-glossário)

## 1. Resumo

Este modelo tem por objetivo calcular o valor diário de subsídio para as empresas do Serviço Público de Transporte de Passageiros por Meio de Ônibus do Município do Rio de Janeiro (SPPO).

A metodologia atual é composta por 2 etapas principais:

1. **Percentual de operação diário (POD)**, que define se o subsídio será ou não pago (ou mesmo se o operador será penalizado); 
2. **Quilometragem realizada por tipo de viagem**, que define o quanto será pago.
3. **Remuneração total da linha**, conforme 1 e 2.

Os cálculos se aplicam individualmente para cada linha e cada dia de operação.

## 2. Etapas

### 2.1. Percentual diário de operação (POD)

O percentual é definido como a razão entre a quilometragem realizada ($QR$) e a quilometragem planejada ($QP$) do serviço, sendo:

$$ POD = 100 \times \frac{QR}{QP} $$

Esse número determina se o subsídio será pago (conforme decreto [DECRETO RIO N.o 51.889/2022](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2022/12/DECRETO_RIO_No_51889_DE_26_DE_DEZEMBRO_DE_2022.pdf)) ou se a linha será penalizada por má operação (conforme decreto [DECRETO RIO N.o 51.940/2023](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2023/01/Decreto-51940-de-16-de-janeiro-de-2023.pdf)). A tabela de classificação do POD segue abaixo:

| Intervalo do POD | Classificação | Tipo de Penalidade  | Valor (R$) |
|------------------|--------------------|-------------|------------|
| >= 80% | Operação subsidiada | - | A definir (tipo de viagem)* |
| < 80% | Operação não subsidiada | 0.00 |
| < 60% | Má operação - Penalidade Média | 563.28* |
| < 40% | Má operação - Penalidade Grave | 1126.55* |

\* O valor do subsídio para cada dia e tipo de viagem é definido na tabela `valor_tipo_viagem`, assim como os valores de penalidade estão em `valor_tipo_penalidade`. Esses valores são definidos conforme a legislação vigente.

### 2.2. Quilometragem realizada por tipo de viagem (QR)

A quilometragem por tipo de viagem é a dada pela multiplicação da quantidade de viagens realizadas daquele tipo ($V_i$) pela extensão ($d$) do itinerário, sendo:

$$ QR_i = V_i \times d $$

Essa quilometragem é registrada na tabela `sumario_servico_dia_tipo`. Os tipos de viagem considerados são definidos pelo estado do veículo que operou a viagem, podendo ser:

- `Licenciado com ar e autuado (023.II)`: viagem realizada por veículo com ar que foi autuado operando com ar condicionado desligado (código de autuação: 023.II);
- `Licenciado com ar e não autuado (023.II)`: viagem realizada por veículo com ar que não foi autuado;
- `Licenciado sem ar`: viagem realizada por veículo sem ar condicionado instalado;
- `Não licenciado`: viagem realizada por veículo não licenciado na SMTR*

\* Consta para fins de cálculo da QR e POD, mas é desconsiderado para remuneração.

### 2.3. Remuneração total

A remuneração total é calculada combinando o percentual de operação (POD) com a quilometragem realizada (QR) de cada linha.

- Caso POD < 80%, a penalidade aplicada segue a tabela da seção 2.1;
- Caso POD >= 80%, o subsídio é dado pela soma da QR de cada tipo de viagem pelo seu valor, sendo:


| Tipo de Viagem ($i$) | Valor (R$/km) *
|------------------|---|
| `Licenciado com ar e autuado (023.II)` | 0.00 |
| `Licenciado com ar e não autuado (023.II)` | 2.81 |
| `Licenciado sem ar` | 1.97 |


\* Os valores atuais refletem a [RESOLUÇÃO RIO N.o 3.591/2023](https://transportes.prefeitura.rio/wp-content/uploads/sites/31/2023/02/RESOLUCAO-SMTR-No-3591-DE-01-DE-FEVEREIRO-DE-2023.pdf), conforme legislação viagente.

## 3. Glossário

- **Empresa**: Empresa responsável pela operação dos itinerários das linhas. As emmpresas são organizadas em 4 consórcios (Transcarioca, Santa Cruz, Internorte e Intersul), que operam linhas com exclusividade (uma empresa pode operar uma mesma linha que outra, mas apenas um único consórcio).
- **Linha**: Conjunto de serviços, usualmente descrito por números, ex: 123, 720.
- **Serviço**: Conjunto de itinerários, descritos pela linha e identificador do serviço, ex: 457, SV606. As linhas podem ter serviços normais e específicos (como 'Variante', 'Parcial', etc).
- **Itinerário**: Trajetos executados por veículos que param em pontos específicos, determinados por um serviço (ex: 457) e sentido (ex: 457 - Copacabana x 457 - Abolição).
