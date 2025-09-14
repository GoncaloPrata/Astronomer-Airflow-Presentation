"""
## Tabelas com Dependencias

Esta DAG tem como propósito ilustrar como é que pode ser feito o processo de execução
de um processo de ETL.

"""

# ------- #
# Imports #
# ------- #

from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from pendulum import datetime, duration
from airflow.sdk import dag, task
from airflow.operators.python import get_current_context
from time import sleep

# --------------------------------- #
# List of Tables and their Children #
# --------------------------------- #

# Less complex version
tabelas = {
    "tabela1" : ["tabela2", "tabela3"],
    "tabela2" : ["tabela3"],
    "tabela3" : []  
}

# More complex version
"""
tabelas = {
    "tabela1" : ["tabela2", "tabela3", "tabela4"],
    "tabela2" : ["tabela4", "tabela7"],
    "tabela3" : ["tabela5"],
    "tabela4" : [],
    "tabela5" : ["tabela7"],
    "tabela6" : ["tabela7"],
    "tabela7" : []
}
"""

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

# -------- #
# DAG Flow #
# -------- #

def tabelas_com_deps():

    task_objects = {}

    for tabela in tabelas.keys():

        # --------- #
        # DAG Tasks #
        # --------- #

        @task(task_id=f"ver_ultima_run_{tabela}")
        def check_last_run(table_name=tabela) -> None:
            print(f"Checking last run of {table_name}")
        
        @task(task_id=f"correr_{tabela}")
        def run_table(table_name=tabela):
            chars = ascii_uppercase + ascii_lowercase + digits
            exec_id = 'exec_id_' + ''.join(choice(chars) for _ in range(12))
            print(f"Running task for {table_name}. The execution id is : {exec_id}")
            ti = get_current_context()["ti"]
            ti.xcom_push(key=f"correr_{tabela}_exec_id", value=exec_id)

        @task(task_id=f"verificar_estado_da_run_{tabela}")
        def check_progress_of_run(table_name=tabela) -> None:
            ti = get_current_context()["ti"]
            result = ti.xcom_pull(key=f"correr_{tabela}_exec_id", task_ids=f"correr_{tabela}")
            print(f"Checking status of run with execution id : {result}.")
            sleep(10)
            print("The run ended with success.")
        
        task_objects[tabela] = [ check_last_run() , run_table() , check_progress_of_run() ]

    # -------- #
    # DAG Flow #
    # -------- #

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