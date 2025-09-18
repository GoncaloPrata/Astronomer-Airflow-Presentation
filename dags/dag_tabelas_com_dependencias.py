"""
## Notebook Execution Dependency DAG

This DAG simulates executing multiple notebooks in a specific order 
and passing information between tasks using Airflow’s XCom feature.

Each task represents a notebook execution, and the DAG ensures that 
notebooks run in the correct sequence. Data or status from one task 
is passed to the next using XComs, demonstrating inter-task communication 
and dependency management.

Each of the tasks executed for a given "tabela" represents one step of
an etl process:

1. Trigger the notebook that validates if the last run of the table ended with success
2. Trigger the notebook that runs the table
3. Verifies if the execution is still running, has failed, or has ended with success.

"""


# ------- #
# Imports #
# ------- #

from random import choice, randint
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
    dag_id="dag_tabelas_com_dependencias",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",  
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=5),
    },
    tags=["demo", "tables", "dependencies_between_taskss"],
    is_paused_upon_creation=True,
    catchup=False
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

        @task(
            task_id=f"ver_ultima_run_{tabela}",
            retries = 2,
            trigger_rule = "always"
        )
        def check_last_run(table_name=tabela) -> None:
            print(f"Checking last run of {table_name}")
        
        @task(
            task_id=f"correr_{tabela}",
            retries = 2
        )
        def run_table(table_name=tabela):

            # Generate a random string of 12 characters
            chars = ascii_uppercase + ascii_lowercase + digits
            exec_id = 'exec_id_' + ''.join(choice(chars) for _ in range(12))

            print(f"Running notebook to execute table '{table_name}'. The execution id is : '{exec_id}'.")

            # Pushes the "exec_id" variable to the XCom so that it can be used in other tasks.
            ti = get_current_context()["ti"]
            ti.xcom_push(key=f"correr_{table_name}_exec_id", value=exec_id)

        @task(task_id=f"verificar_estado_da_run_{tabela}",
              retries = 10)
        def check_progress_of_run(table_name=tabela) -> None:

            # Retrieve the "exec_id" from the XCom
            ti = get_current_context()["ti"]
            exec_id_from_xcom = ti.xcom_pull(key=f"correr_{table_name}_exec_id", task_ids=f"correr_{table_name}")
            
            print(f"Checking status of run with execution id : {exec_id_from_xcom}.")
            sleep(10)
            if randint(1, 100) >= 25:
                print("The execution is not yet done.")
                raise ValueError("The run has not yet ended.")
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