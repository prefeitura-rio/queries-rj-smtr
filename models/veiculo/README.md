# Veículo
* Versão: 1.0.0
* Data de início: 16/01/2023

![veiculo](https://user-images.githubusercontent.com/66736583/227351937-7fe4113b-1d94-425d-a170-7e49ed40d66e.png)

<div align="justify">
  
  ## Método

  ### 1. Captura de Dados
  Diariamente são obtidos os [dados de infrações](https://www.data.rio/documents/multas-aplicadas-aos-modos-de-transporte-nos-últimos-cinco-anos) e [dados cadastrais dos veículos](https://www.data.rio/documents/dados-cadastrais-dos-veículos-que-operam-o-sistema-de-ônibus-brt-e-sppo) que operam o sistema de ônibus BRT e o Serviço Público de Transporte de Passageiros por Meio de Ônibus do Município do Rio de Janeiro (SPPO). Esses dados podem ser obtidos diretamente do site da Prefeitura da Cidade do Rio de Janeiro (PCRJ), que disponibiliza essas informações publicamente através do portal DATA.Rio.

  Após a captura, é realizado um processo de limpeza, validação e materialização desses dados no datalake da Secretaria Municipal de Transportes (SMTR) nas tabelas `veiculo.sppo_licenciamento_stu` e `veiculo.sppo_infracao`.

  Os dados da tabela `veiculo.sppo_licenciamento_stu` tem como origem o Sistema de Transportes Urbanos (STU) da PCRJ/SMTR. Como há um interstício de tempo entre a solitação das empresas operadoras e a efetiva inclusão no STU, esses pedidos são considerados temporariamente na tabela `veiculo.sppo_licenciamento_solicitacao`.

  Para considerar tanto as informações da tabela `veiculo.sppo_licenciamento_stu` quanto da `veiculo.sppo_licenciamento_solicitacao`, essas duas tabelas são aglutinadas, gerando a `veiculo.sppo_licenciamento`.

  ### 2. Classificação Diária dos Veículos
  Na sequência, são considerados todos os veículos identificados na operação (através da tabela `br_rj_riodejaneiro_veiculos.gps_sppo`) e, considerando tanto os dados da tabela `veiculo.sppo_infracao` quanto da tabela `veiculo.sppo_licenciamento`, é gerada a tabela `veiculo.sppo_veiculo_dia`.

  Nesta tabela, há uma relação diária de operação do veículo e seu respectivo status para aquela data, a saber:
  * `Nao licenciado` - caso o veículo não seja identificado na tabela `veiculo.sppo_licenciamento`;
  * `Licenciado sem ar` - caso o veículo não tenha sido licenciado com ar condicionado;
  * `Licenciado com ar e não autuado (023.II)` - caso o veículo tenha sido licenciado com ar condicionado e não tenha sido identificada nenhuma infração relativa ao código `023.II - ART 023 INC II DEC 36343/12 - Inoperância ou mau funcionamento do sistema de ar condicionado, nos veículos em que seja exigida a utilização do equipamento` na data;
  * `Licenciado com ar e autuado (023.II)` - caso o veículo tenha sido licenciado com ar condicionado e não tenha sido identificada alguma infração relativa ao código `023.II` na data.
</div>
