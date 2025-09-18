"""
## Branching Paths DAG

This DAG demonstrates conditional task execution using Airflow’s 
BranchPythonOperator. Depending on a condition or factor, the DAG will execute 
one path or another.

"""


from datetime import datetime
import random

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime, duration

# Function to decide which path to take
def choose_path():
    # Simulate a condition: randomly pick 'path_a_task1' or 'path_b_task1'
    num = random.randint(1, 100)
    print(f"Number generated: {num}")
    chosen = "path_a_task1" if num <= 50 else "path_b_task1"
    print(f"Chosen path: {chosen}")
    return chosen

@dag(
    dag_id="dag_branching_paths",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",  
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=5),
    },
    is_paused_upon_creation=True,
    tags=["demo", "branching", "taskflow"],
)
def branching_paths_dag():

    start = EmptyOperator(task_id="start")

    # Branching operator
    branch = BranchPythonOperator( # Expects either a task_id or a list of task_ids to follow through
        task_id="branching_decision",
        python_callable=choose_path,
    )

    # Path A tasks
    @task()
    def path_a_task1():
        print("Executing Path A - Task 1")

    @task()
    def path_a_task2():
        print("Executing Path A - Task 2")

    # Path B tasks
    @task()
    def path_b_task1():
        print("Executing Path B - Task 1")

    @task()
    def path_b_task2():
        print("Executing Path B - Task 2")

    # Join task to continue DAG after branching
    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # ensures DAG continues even if one path is skipped
    )

    path_a_task_1 = path_a_task1()
    path_a_task_2 = path_a_task2()
    path_b_task_1 = path_b_task1()
    path_b_task_2 = path_b_task2()

    # DAG structure
    start >> branch
    branch >> [ path_a_task_1 , path_b_task_1 ]
    path_a_task_1 >> path_a_task_2 >> join
    path_b_task_1 >> path_b_task_2 >> join

# Instantiate the DAG
branching_paths_dag()
