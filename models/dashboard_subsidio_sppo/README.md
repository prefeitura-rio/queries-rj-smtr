# Dashboard Subsídio SPPO
* Versão: 1.0.0
* Data de início: 16/01/2023

![dashboard_subsidio_sppo](https://user-images.githubusercontent.com/66736583/227094879-fc446ea3-e1c5-483e-94e7-51b2f76c4f19.png)

<div align="justify">
  
  ## Etapas

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

  ### 3. Sumarização das Informações
  Com base nas informações da tabela `veiculo.sppo_veiculo_dia`, as viagens são sumarizadas na tabela `dashboard_subsidio_sppo.sumario_servico_dia`, considerando os diferentes valores para cada tipo de situação dos veículos, conforme [legislação em vigor](https://transportes.prefeitura.rio/subsidio).

  Adicionalmente, é calculado também penalidade caso haja redução da operação a patamares inferiores a 60% da quilometragem determinada pela PCRJ/SMTR cada linha do SPPO-RJ.
  <ol type="I">
  <li>Linha com operação entre 40% e 60% da quilometragem estipulada - penalidade equivalente a uma infração média prevista no Código Disciplinar do SPPO.
  </li>
  <li>Linha com operação inferior a 40% da quilometragem estipulada - penalidade equivalente a uma infração grave prevista no Código Disciplinar do SPPO.</li>
  </ol>
</div>
