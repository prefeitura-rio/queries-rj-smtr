# Queries SMTR 🚍🔎

> Repositório adaptado do template do [Escritório de
> Dados](https://github.com/prefeitura-rio/queries) para versionamento e
> execução de projetos no datalake.

## Requerimentos

* Python <=3.9

## Desenvolvimento (local)

### Iniciando o ambiente

* Crie um ambiente virtual e instale as dependências:

```bash
python -m venv dbt-env
. dbt-env/Scripts/activate
python -m pip install --upgrade pip 
pip install -r requirements-dev.txt
```

* Configure suas credenciais para leitura/escrita no datalake:

```bash
# copie o arquivo de exemplo
cp dev/profiles-example.yml dev/profiles.yml
# preencha com suas credenciais
```

* Edite o arquivo [`dev/run.py`](dev/run.py) para rodar seus testes. Em
  seguida, execute o script:

```bash
python dev/run.py
```

## Adicionando dados

### Novo conjunto

1. Crie uma branch com o mesmo padrão da pipeline correspondente em
   [pipelines](https://github.com/prefeitura/pipelines)(quando houver)

2. Crie um novo diretório `models/<dataset-id>`, sendo `dataset_id` o
   nome do conjunto. Nesta pasta serão guardadas as queries (modelos) que dão
   origem às tabelas deste dataset no BigQuery.

3. No arquivo `dbt_project.yml`, adicione o `dataset-id` junto aos
   conjuntos já registrados, conforme abaixo:

```yaml
models:
  rj-smtr:
    <dataset-id>:
      +materialized: view # Materialization type (view, table or incremental)
      +schema: <dataset-id> # Overrides the default schema (defaults to what is set on profiles.yml)
```

### Novas tabelas

Crie os modelos que desejar em `models/<dataset-id>` (ex:
`nome_da_tabela.sql`). Nesses arquivos, adicione o código SQL utilizado
para gerar as tabelas no BigQuery. Quaisquer especificações de particionamento
também devem ser inseridas ali.

Leia:

* [Tipos de tabela do dbt](https://docs.getdbt.com/docs/build/materializations)
* [Configurações de particionamento no dbt](https://docs.getdbt.com/reference/resource-configs/bigquery-configs)

#### Para publicar no datario

**Antes de fazer o merge da branch, garanta que os devidos metadados
para a(s) nova(s) tabela(s) estão preenchidos no portal
<https://meta.dados.rio/>**. Caso ontrário, não será gerada a documentação
da tabela.
