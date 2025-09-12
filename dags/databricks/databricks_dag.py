"""
## Fabric DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

"""

 # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.sdk import dag, task
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

def databricks_dag():

    task_objects = {}

    for tabela in tabelas.keys():
        
        @task(task_id=f"correr_{tabela}")
        def run_table(table_name=tabela):
            print(f"Running task for {table_name}")
        
        @task(task_id=f"ver_ultima_run_{tabela}")
        def check_last_run(table_name=tabela) -> None:
            print(f"Checking last run of {table_name}")
        
        task_objects[tabela] = check_last_run() >> run_table()

    for tabela, dependencies in tabelas.items():
        for dep in dependencies:
            #if dep in task_objects:  # only link tasks that exist
            task_objects[tabela] >> task_objects[dep]


# Instantiate the DAG
databricks_dag()
