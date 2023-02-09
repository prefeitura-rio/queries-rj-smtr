from utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

# run_dbt_model(
#     dataset_id="operacao",
#     _vars={"end_date": "2023-01-31"}
# )

run_dbt_model(
    dataset_id="projeto_subsidio_sppo",
    table_id="viagem_completa",
    upstream=True,
    exclude="+gps_sppo",
    _vars={"run_date": "2023-01-31"}
)