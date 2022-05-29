import os
from datetime import datetime as dt
from datetime import timedelta

run_date = dt.strptime("2022-05-16", "%Y-%m-%d")
for i in range(0, 6):
    date = run_date + timedelta(days=i)
    print(f"\n=========> DATA: {date}")
    command = f"dbt run --profiles-dir . --vars \"{{'run_date':{date}}}\" --select +viagem_completa" # --exclude sumario_viagem_completa
    print("=========> Running: ", command)
    os.system(command)