from utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

# run_dates = [f"2022-12-{i:02}" for i in range(18,31)]

# for date in run_dates:
#     run_dbt_model(
#         dataset_id="projeto_subsidio_sppo",
#         table_id="viagem_completa",
#         upstream=True,
#         exclude="+gps_sppo",
#         _vars={"run_date": date},
#     )

# run_dbt_model(
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="sumario_subsidio_dia",
#     _vars={"end_date": "2022-12-30"},
# )

# run_dbt_model(
#     dataset_id="dashboard_subsidio_sppo",
#     _vars={"run_date": "2022-12-31"},
# )

# run_dbt_model(
#     dataset_id="projeto_subsidio_sppo",
#     table_id="aux_registros_status_trajeto",
#     downstream=True,
#     # _vars={"recurso_viagem_start": "2022-06-01", 
#     #        "recurso_viagem_end": "2022-06-30", 
#     #        "recurso_timestamp_captura": "2022-07-05T00:00:00"}
#     _vars = {"run_date": "2022-10-03"},
#     #_vars = {"run_date": "2022-12-02"},
#     exclude = "sumario_subsidio_dia_periodo_recurso+ sumario_subsidio_dia+"
# )


#run_dates = [f"2022-12-{i:02}" for i in range(2,32)]
# run_dates = ["2023-01-01"]
# run_dates = pd.date_range(start="2022-10-01", end="2022-11-01").to_list()

# for date in run_dates:
#     run_dbt_model(
#         dataset_id="projeto_subsidio_sppo",
#         table_id="aux_registros_status_trajeto",
#         downstream=True,
#         _vars = {"run_date": date},
#         exclude = "sumario_subsidio_dia_periodo_recurso+ sumario_subsidio_dia+ sumario_subsidio_dia_periodo+ dashboard_subsidio_sppo+"
#     )

    # run_dbt_model(
    #     dataset_id="projeto_subsidio_sppo",
    #     table_id="viagem_completa",
    #     upstream=True,
    #     _vars = {"run_date": date},
    #     exclude = "+gps_sppo"
    # )

# run_dbt_model(
#     dataset_id="projeto_subsidio_sppo",
#     # table_id="aux_registros_status_trajeto",
#     # downstream=True,
#     table_id="viagem_completa",
#     upstream=True,
#     _vars = {"run_date": "2022-12-02"},
#     #exclude = "sumario_subsidio_dia_periodo_recurso+ sumario_subsidio_dia+ sumario_subsidio_dia_periodo+"
#     exclude = "+gps_sppo"
# )


#run_dates = [f"2023-01-{i:02}" for i in range(2,32)]
#run_dates = ["2023-01-01", "2023-01-02"]
# run_dates = [f"2023-01-{i:02}" for i in range(1,17)]

# for date in run_dates:
#     run_dbt_model(
#         dataset_id="projeto_subsidio_sppo",
#         table_id="viagem_completa",
#         upstream=True,
#         _vars = {"run_date": date},
#         exclude = "+gps_sppo"
#     )

run_dbt_model(
    dataset_id="projeto_subsidio_sppo",
    table_id="subsidio_data_versao_efetiva",
)