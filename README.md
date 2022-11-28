# Queries SMTR

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
. dbt-env/bin/activate
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
