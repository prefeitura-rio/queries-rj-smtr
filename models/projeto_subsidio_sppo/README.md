# Subsídio SPPO

* Versão: 1.0.0
* Data de início: 01/06/2022

<!-- <img width="1283" alt="image"
src="https://user-images.githubusercontent.com/20743819/172705939-b4afdb7d-f11f-454e-9dd1-68c1f447ca47.png"> -->

<!-- ## Descrição -->

## Etapas

### 1. Atualização de viagens planejadas

<img width="995" alt="image"
src="https://user-images.githubusercontent.com/20743819/179116129-8f8d56d2-97c8-4e5b-b490-12c58d39bc80.png">

Os serviços e trajetos considerados no subsídio são
atualizados pela equipe de Planejamento,
podendo ser incluídas novas linhas, alteradas rotas ou
mesmo a quilometragem determinada. Essa
rotina acontece a cada quinzena de apuração do subsídio.

Como resultado, obtemos para cada dia de apuração o quadro planejado por
trajeto, conforme exemplo abaixo. Nele, consolidamos:

1. A distância planejada para cada viagem
(`distancia_planejada`, ou extensão do trajeto) e total por dia (`distancia_total_planejada`)

<img width="813" alt="image"
src="https://user-images.githubusercontent.com/20743819/179118003-d97f632c-810b-4f0f-a4ec-825e5d25f01a.png">

2. A geometria do trajeto (`shape`), assim como seu ponto inicial
   (`start_pt`) e final (`end_pt`),
   que serão usados para identificar as posições de GPS na próxima
   etapa.

<img width="1250" alt="image" src="https://user-images.githubusercontent.com/20743819/179119285-063df325-f1d9-4736-b37b-9e0eeaf3b8a7.png">

**Observações**:

* Os trajetos circulares têm seu shape dividido em ida e volta, apa
  possibilitar a identificação da viagem. Nestes casos, a coluna
  `shape_id_planejado` recebe o id "teórico" e `shape_id` recebe o ID
  de ida ou volta, trocando-se 11º caractere do ID [ex: 866 -
  `O0866AAA0ACDU01` (teórico) x `O0866AAA0AIDU01` (ida)].

* O `tipo_dia` determina a quilometragem total planejada para aquele dia, conforme o [Plano Operacional](https://transportes.prefeitura.rio/subsidio/). Caso o dia seja um feriado, o `tipo_dia` considerado será de Domingo. Caso seja ponto facultativo, será usado `Sabado`.

* Nesta versão, os horários de `inicio_periodo` e `fim_periodo` são desconsiderados e consideramos o planejado para o dia inteiro.

### 2. Cálculo de viagens realizadas

<img width="1026" alt="image"
src="https://user-images.githubusercontent.com/20743819/179116020-37472af3-0b3e-4e94-862a-9c3bbb01f088.png">

O cálculo é realizado cruzando sinais de GPS com o trajeto planejado de
cada serviço. Em resumo, identifica-se potenciais viagens a partir de posições
do GPS emitidas nos pontos inicial e final do trajeto, e depois valida-se a
viagem caso atinja os percentuais de conformidade mínimos de:

* **Cobertura de GPS**: 50% dos minutos entre o início e fim da viagem devem ter pelo menos 1 sinal de GPS;
* **Cobertura do trajeto**: 80% das posições de GPS devem ter sido identificadas dentro
  do trajeto planejado (num raio de 500m);

O passo a passo do algoritmo está descrito abaixo.

> Vamos seguir um exemplo com o ônibus B63050 (`id_veiculo`) no
> serviço `349` ao longo da metodologia para facilitar a explicação.
> <img width="367" alt="Screen Shot 2022-07-14 at 21 17 15"
src="https://user-images.githubusercontent.com/20743819/179121947-3bc63cfd-9f81-4ce7-be05-887ee4b6fe61.png">

#### 2.1. Classificação das posições de GPS no trajeto (`aux_registros_status_trajeto`)

As posições de GPS dos ônibus são capturadas a cada minuto, e
posteriormente tratadas a cada hora na tabela
[`gps_sppo`](). A partir dos dados de GPS, sabemos para cada veículo
(`id_veiculo`, ou número de ordem) e datahora (`timestamp_gps`), qual era sua posição
(`latitude`, `longitude`) e o serviço no qual estava operando (`servico`)
naquele momento.

> Para o dia 24/06, recebemos as seguintes informações por GPS do B63050 de 6:15 às 6:16:
<img width="491" alt="image"
src="https://user-images.githubusercontent.com/20743819/179122550-1d502871-7b4f-4b1e-bd7f-0a5959dad565.png">

Cruzamos essa tabela de posições de GPS com o trajeto (`shape`) da
`viagem_planejada` pela data e serviço para classificar cada
posição como:

* `start`: veículo estava no ponto inicial do trajeto (num raio de 500m)
* `end`: ponto final do trajeto (num raio de 500m)
* `middle`: meio do trajeto (num raio de 500m)
* `out`: fora do trajeto (num raio de 500m)

Nesta etapa, as posições são
duplicadas para os trajetos de ida (`I`) e volta (`V`)
pois ainda não temos como dizer qual sentido o veículo está operando.

> Uma vez classificado o `status_viagem`, obtemos para o mesmo intervalo
> de 6:15 às 6:16:
> <img width="819" alt="image" src="https://user-images.githubusercontent.com/20743819/179124464-9f08c16b-0272-4941-a7f8-45f8d5fe5394.png">

#### 2.2. Identificação de início e fim de viagens (`aux_viagem_inicio_fim`, `aux_viagem_circular`)

Uma vez classificadas as posições, buscamos os pares de início e fim que
possivelmente formam uma viagem.

Para isso, identificamos a "movimentação" do veículo: qual é seu
`status_viagem` naquele momento e qual era seu `status_viagem` imediatamente
anterior.
Classificamos, então, como início da viagem (`datetime_partida`) o momento em que o veículo
sai do ponto inicial e entra no trajeto, isto é, movimentação =
`startmiddle`. Da mesma forma, o fim da viagem é classificado como o
momento em que o veículo chega no ponto final a partir do trajeto, isto
é, movimentação = `middleend`.

> Na seção anterior vimos que o B63050 esteve no ponto inicial
(`start`) do trajeto de volta (`V`) às 6:15:26 e logo em seguida, às
6:15:56, esteve no meio do trajeto (`middle`). Logo, 6:15:26 é
potencialmente o início de uma viagem no serviço 349. Para afirmarmos isso, deve haver posteriormente uma movimentação `middleend`,
que é observada às 7:20:57. Realizando esse processo ao longo do dia
24/06, obtemos as seguintes possíveis viagens do B63050:
> <img width="898" alt="image"
> src="https://user-images.githubusercontent.com/20743819/179125623-af731b85-0f9d-4d00-bada-93ca5997dea8.png">

Realizamos um tratamento final nessa etapa para juntar as viagens circulares, separadas
nos trajetos de ida (`I`) e volta (`V`), na tabela `aux_viagem_circular`.
O início de uma viagem circular (`datetime_partida`) corresponde ao
início do trajeto de ida e o final da viagem (`datetime_chegada`) ao
final do trajeto de volta.

#### 2.3. Classificação das posições de GPS nas viagens (`registros_status_viagem`)

#### 2.4. Cálculo dos percentuais de conformidade da viagem (`viagem_conformidade`, `viagem_completa`)

### 3. Sumarização de viagens

<img width="1341" alt="image" src="https://user-images.githubusercontent.com/20743819/179116232-fb73d399-068c-4a79-8165-733c01f597ea.png">
