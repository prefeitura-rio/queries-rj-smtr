from utils import run_dbt_model
import os

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="gtfs",
    flags="--full-refresh",
)