from utils import run_dbt_model
# import os

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="br_rj_riodejaneiro_gtfs",
    table_id="agency_gtfs",
    _vars={'data_versao_gtfs': '2023-10-06'}
)
