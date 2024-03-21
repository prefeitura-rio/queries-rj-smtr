from utils import run_dbt_model
import os

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="projeto_subsidio_sppo",
    table_id="subsidio_data_versao_efetiva",
    _vars={"run_date": "2024-03-18"}
)
