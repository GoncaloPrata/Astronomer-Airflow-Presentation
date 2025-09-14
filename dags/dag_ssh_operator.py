"""

## Bash Operator DAG

This DAG demonstrates how Astronomer Airflow can execute Bash commands 
directly as tasks. It runs a sequence of Bash commands to process 
CSV files.

"""

from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from pendulum import datetime

# Paths
INPUT_PATH = "/usr/local/airflow/include/bank_data.csv"
OUTPUT_DIR = "/usr/local/airflow/include/output"

@dag(
    dag_id="dag_ssh_operator",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["csv", "bash", "example"],
)

def dag_ssh_operator():

    # 1. Wait until the CSV file exists
    wait_for_csv = FileSensor(
        task_id="wait_for_csv",
        fs_conn_id="fs_default",
        filepath="include/bank_data.csv",
        poke_interval=10,   # check every 10s
        timeout=600,        # fail after 10 min if file not found
        mode="poke",
    )

    create_output_dir_if_not_exists = BashOperator(
        task_id = "create_out_dir_if_not_exist",
        bash_command = f"mkdir -p {OUTPUT_DIR}"
    )

    content_of_output_directory = BashOperator(
        task_id = "out_dir_content",
        bash_command = f"ls -l --color=auto --time-style=long-iso --human-readable -o {OUTPUT_DIR}"
    )

    # 2. Filtering tasks using BashOperator + awk
    filter_name_id = BashOperator(
        task_id="filter_for_name_and_id",
        bash_command=f"awk -F, 'NR>1 {{print $1, $2}}' {INPUT_PATH} > {OUTPUT_DIR}/name_id.csv"
    )

    filter_name_balance = BashOperator(
        task_id="filter_for_name_and_balance",
        bash_command=f"awk -F, 'NR>1 {{print $1, $3}}' {INPUT_PATH} > {OUTPUT_DIR}/name_balance.csv"
    )

    filter_id_balance = BashOperator(
        task_id="filter_for_id_and_balance",
        bash_command=f"awk -F, 'NR>1 {{print $2, $3}}' {INPUT_PATH} > {OUTPUT_DIR}/id_balance.csv"
    )

    filter_balance_gt_300 = BashOperator(
        task_id="filter_for_balance_greater_than_300",
        bash_command=f"awk -F, 'NR>1 && $3 > 300 {{print $2}}' {INPUT_PATH} > {OUTPUT_DIR}/balance_gt_300.csv"
    )

    filter_balance_lt_300 = BashOperator(
        task_id="filter_for_balance_lesser_than_300",
        bash_command=f"awk -F, 'NR>1 && $3 < 300 {{print $2}}' {INPUT_PATH} > {OUTPUT_DIR}/balance_lt_300.csv"
    )

    # DAG structure: wait first, then all filters run in parallel
    wait_for_csv >> create_output_dir_if_not_exists >> [
        filter_name_id,
        filter_name_balance,
        filter_id_balance,
        filter_balance_gt_300,
        filter_balance_lt_300,
    ] >> content_of_output_directory

dag_ssh_operator()
