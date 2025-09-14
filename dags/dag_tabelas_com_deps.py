"""
## Tabelas com Dependencias

Esta DAG tem como propósito ilustrar como é que pode ser feito o processo de execução
de um processo de ETL.

"""

 # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.sdk import dag, task
from pendulum import datetime, duration
from time import sleep

# --------------------------------- #
# List of Tables and their Children #
# --------------------------------- #

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


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",  
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["example", "space"],
    is_paused_upon_creation=True,
)

def tabelas_com_deps():

    task_objects = {}

    for tabela in tabelas.keys():
        
        @task(task_id=f"correr_{tabela}")
        def run_table(table_name=tabela):
            print(f"Running task for {table_name}")
        
        @task(task_id=f"ver_ultima_run_{tabela}")
        def check_last_run(table_name=tabela) -> None:
            print(f"Checking last run of {table_name}")

        @task(task_id=f"verificar_estado_da_run_{tabela}")
        def check_progress_of_run(table_name=tabela) -> None:
            print("Checking status of current run.")
        
        task_objects[tabela] = [ check_last_run() , run_table() , check_progress_of_run() ]

    for tabela, dependencies in tabelas.items():
        for dep in dependencies:
            
            # Check Last Exec Parent -> Exec Parent -> Monitor Exec Parent 
            task_objects[tabela][0] >> task_objects[tabela][1] >> task_objects[tabela][2]
            # Check Last Exec Child -> Exec Child -> Monitor Exec Child
            task_objects[dep][0] >> task_objects[dep][1] >> task_objects[dep][2] 
            # After triggering Parent Exec you can do the pre check of the child
            task_objects[tabela][1] >> task_objects[dep][0]
            # The Child Exec can only start after both the Parent Run and Check Last Exec Child has ended 
            [task_objects[tabela][2] , task_objects[dep][0] ] >> task_objects[dep][1]


# Instantiate the DAG
tabelas_com_deps()
