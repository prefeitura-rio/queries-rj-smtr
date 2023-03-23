# Dashboard Subsídio SPPO

* Versão: 1.0.0
* Data de início: 16/01/2023

<!-- <img width="1283" alt="image"
src="https://user-images.githubusercontent.com/20743819/172705939-b4afdb7d-f11f-454e-9dd1-68c1f447ca47.png"> -->

<!-- ## Descrição -->

<img width="995" alt="image"
src="https://user-images.githubusercontent.com/20743819/179116129-8f8d56d2-97c8-4e5b-b490-12c58d39bc80.png">

## Etapas

### 1. Captura de Dados
Diariamente são obtidos os [dados de infrações](https://www.data.rio/documents/multas-aplicadas-aos-modos-de-transporte-nos-últimos-cinco-anos) e [dados cadastrais dos veículos](https://www.data.rio/documents/dados-cadastrais-dos-veículos-que-operam-o-sistema-de-ônibus-brt-e-sppo) que operam o sistema de ônibus BRT e SPPO. Esses dados podem ser obtidos diretamente do site da Prefeitura da Cidade do Rio de Janeiro (PCRJ), que disponibiliza essas informações publicamente através do portal DATA.Rio.

Após a captura, é realizado um processo de limpeza, validação e materialização desses dados no datalake da Secretaria Municipal de Transportes (SMTR) nas tabelas `rj-smtr.veiculo.sppo_licenciamento_stu` e `rj-smtr.veiculo.sppo_infracao`.

Os dados da tabela `rj-smtr.veiculo.sppo_licenciamento_stu` tem como origem o Sistema de Transportes Urbanos (STU) da PCRJ/SMTR. Como há um interstício de tempo entre a solitação das empresas operadoras e a efetiva inclusão no STU, esses pedidos são considerados temporariamente na tabela `rj-smtr.veiculo.sppo_licenciamento_solicitacao`.

Para considerar tanto as informações da tabela `rj-smtr.veiculo.sppo_licenciamento_stu` quanto da `rj-smtr.veiculo.sppo_licenciamento_solicitacao`, essas duas tabelas são aglutinadas, gerando a `rj-smtr.veiculo.sppo_licenciamento`.

### 2. Classificação Diária dos Veículos
Na sequência, são considerados todos os veículos identificados na operação (através da tabela `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`) e, considerando tanto os dados da tabela `rj-smtr.veiculo.sppo_infracao` quanto da tabela `rj-smtr.veiculo.sppo_licenciamento`, é gerada a tabela `rj-smtr.veiculo.sppo_veiculo_dia`.

Nesta tabela, há uma relação diária de operação do veículo e seu respectivo status para aquela data, a saber:
* `Nao licenciado` - caso o veículo não seja identificado na tabela `rj-smtr.veiculo.sppo_licenciamento`;
* `Licenciado sem ar` - caso o veículo não tenha sido licenciado com ar condicionado;
* `Licenciado com ar e não autuado (023.II)` - caso o veículo tenha sido licenciado com ar condicionado e não tenha sido identificada nenhuma infração relativa ao código `023.II - ART 023 INC II DEC 36343/12 - INOPERÂNCIA OU MAU FUNCIONAMENTO DO SISTEMA DE AR CONDICIONADO, NOS VEÍCULOS EM QUE SEJA EXIGIDA A UTILIZAÇÃO DO EQUIPAMENTO` na data;
* `Licenciado com ar e autuado (023.II)` - caso o veículo tenha sido licenciado com ar condicionado e não tenha sido identificada alguma infração relativa ao código `023.II` na data.

### 3. Sumarização das Informações
Com base nas informações da tabela `rj-smtr.veiculo.sppo_veiculo_dia`, as viagens são sumarizadas na tabela `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia`, considerando os diferentes valores para cada tipo de situação dos veículos, conforme [legislação em vigor](https://transportes.prefeitura.rio/subsidio).

Adicionalmente, é calculado também penalidade caso haja redução da operação a patamares inferiores a 60% da quilometragem determinada pela PCRJ/SMTR cada linha do SPPO-RJ.
<ol type="I">
<li>Linha com operação entre 40% e 60% da quilometragem estipulada - penalidade equivalente a uma infração média prevista no Código Disciplinar do Serviço Público de Transporte de Passageiros por Meio de Ônibus do Município do Rio de Janeiro - SPPO.
</li>
<li>Linha com operação inferior a 40% da quilometragem estipulada - penalidade equivalente a uma infração grave prevista no Código Disciplinar do Serviço Público de Transporte de Passageiros por Meio de Ônibus do Município do Rio de Janeiro - SPPO.</li>
</ol>