import os
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd

from typing import Any, Dict, List, Union


def run_dbt_model(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Run a DBT model.
    """
    run_command = "dbt run"

    common_flags = "-x --profiles-dir ./dev"

    if not flags:
        flags = common_flags
    else:
        flags = common_flags + " " + flags

    if not model:
        model = f"{dataset_id}"
        if table_id:
            model += f".{table_id}"

    # Set models and upstream/downstream for dbt
    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += f"{model}"
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"
    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")
    os.system(run_command)
