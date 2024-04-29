from utils import run_dbt_model
import os

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="dashboard_subsidio_sppo",
    _vars={
        "end_date": "2024-04-15"
    }
)
