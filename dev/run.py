from utils import run_dbt_model
import os

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="br_rj_riodejaneiro_recursos",
    table_id="recursos_sppo_bloqueio_via",
    upstream=True,
)
