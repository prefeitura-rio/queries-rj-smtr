import os
from datetime import datetime as dt
from datetime import timedelta

run_date = dt.strptime("2022-03-28", "%Y-%m-%d")
for i in range(1, 8):
    command = f"dbt run --profiles-dir . --vars \"{{'run_date':{run_date}}}\" --select " # --exclude sumario_viagem_completa
    print("Running: ", command)
    os.system(command)
run_date += timedelta(days=7)