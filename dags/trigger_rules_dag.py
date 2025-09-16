"""
## Trigger Rules and Email Notification DAG

This DAG explores Airflow’s trigger rules and shows how tasks can be 
executed based on the success or failure of upstream tasks. It includes 
tasks that may fail randomly and a notification task that always runs 
using trigger_rule=all_done.

This DAG is particularly useful for demonstrating resilient workflows 
and how Astronomer Airflow makes monitoring and notification simple.

List of trigger rules : [Airflow trigger rules](https://www.astronomer.io/docs/learn/airflow-trigger-rules)

"""


from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
import random

@task(trigger_rule="all_done")
def task_1():
    if random.choice([True, False]):
        raise ValueError("Task 1 failed randomly!")
    print("Task 1 completed successfully.")

@task(trigger_rule="all_success")
def task_2():
    if random.choice([True, False]):
        raise ValueError("Task 2 failed randomly!")
    print("Task 2 completed successfully.")

@task(trigger_rule="all_failed")
def task_3():
    if random.choice([True, False]):
        raise ValueError("Task 3 failed randomly!")
    print("Task 3 completed successfully.")

@task()
def send_notification():
    # In a real DAG, you would use EmailOperator or SMTP setup
    context = get_current_context()
    dag_run = context["dag_run"]
    print(f"Notification: DAG '{dag_run.dag_id}' finished. Check logs for task statuses.")

@dag(
    dag_id="trigger_rules_dag",
    doc_md=__doc__,
    catchup=False,
    tags=["demo", "trigger_rules", "email", "error_handling"],
)
def trigger_rules_dag():
    start = EmptyOperator(task_id="start")

    t1 = task_1()
    t2 = task_2()
    t3 = task_3()

    # Notification task runs even if some tasks fail
    notify = send_notification()
    notify.trigger_rule = TriggerRule.ALL_DONE  # runs even if upstream fails

    end = EmptyOperator(task_id="end", trigger_rule="always")

    # DAG structure
    start >> [t1, t2, t3] >> notify >> end

trigger_rules_dag()
