"""
# Fabric DAG

Esta DAG é um exemplo das dependências que existem entre tabelas.
Tomemos como exemplo o seguinte cenário: num lakehouse de fabric existem sete tabelas (tabela1 ... tabela7)
que têm dependências entre si (uma tabela tem de ser executada antes que possamos executar a seguinte).

As dependências são as seguintes:

- tabela1 (não tem dependências)
- tabela2 depende das tabelas: 
    - tabela4
    - tabela7
- tabela3 depende das tabelas:
    - tabela5
- tabela4 : (não tem dependências)
- tabela5 depende das tabelas:
    - tabela7
- tabela6 depende das tabelas:
    - tabela7
- tabela7 : (não tem dependências)

"""

 # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration
import requests

# -------------- #
# List of Tables #
# -------------- #

tabelas = {
    "tabela1" : ["tabela2", "tabela3", "tabela4"],
    "tabela2" : ["tabela4", "tabela7"],
    "tabela3" : ["tabela5"],
    "tabela4" : [],
    "tabela5" : ["tabela7"],
    "tabela6" : ["tabela7"],
    "tabela7" : []
}

# -------------- #
# DAG Definition #
# -------------- #


# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2025, 4, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=5),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["example", "space"],  # add tags in the UI
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)

def fabric_dag():

    check_last_run_dict = {}
    run_table_dict = {}
    log_exec = {}

    @task()
    def trigger_next_dag():
        # Trigger DAG 2 (Data Processing)
        trigger = TriggerDagRunOperator(
            trigger_dag_id='databricks_dag'
        )
        return trigger
    
    trig = trigger_next_dag()

    for tabela in tabelas.keys():
        
        @task(task_id=f"correr_{tabela}")
        def run_table(table_name=tabela):
            print(f"Running task for {table_name}")
        
        @task(task_id=f"ver_ultima_run_{tabela}")
        def check_last_run(table_name=tabela):
            print(f"Checking last run of {table_name}")

        @task(task_id=f"log_table_exec_{tabela}", trigger_rule="all_done")
        def log_table_exec(table_name=tabela):
             print(f"Logging execution of table : {table_name}")

        check_last_run_dict[tabela] = check_last_run() 
        run_table_dict[tabela] = run_table()
        log_exec[tabela] = log_table_exec()

    for tabela, dependencies in tabelas.items():
        for dep in dependencies:
                check_last_run_dict[tabela] >> run_table_dict[tabela] >> log_exec[tabela] \
                >> check_last_run_dict[dep] >> run_table_dict[dep] >> log_exec[dep] \
                >> trig

# Instantiate the DAG
fabric_dag()
