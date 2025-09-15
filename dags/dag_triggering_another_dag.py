from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@task()
def prepare_data():
    print("Preparing data for the downstream DAG...")

@task()
def validate_data():
    print("Validating data before triggering the downstream DAG...")

@task()
def finalize():
    print("All steps completed, downstream DAG has been triggered.")

@dag(
    dag_id="dag_triggering_another_dag",
    description="A DAG that triggers another DAG after completing some tasks (Astronomer Airflow 3 style)",
    catchup=False,
    tags=["demo", "dag-dependency", "trigger"],
)
def dag_triggering_another_dag():

    start = EmptyOperator(task_id="start")

    prepare = prepare_data()
    validate = validate_data()

    # Trigger another DAG (by dag_id). Make sure "downstream_dag" exists.
    trigger = TriggerDagRunOperator(
        task_id="trigger_downstream_dag",
        trigger_dag_id="dag_triggered_by_another",  # ID of the DAG you want to trigger
        wait_for_completion=True,         # set to False if you don’t want to block
        reset_dag_run=True,               # create fresh run even if one exists
    )

    finish = finalize()

    # DAG dependencies
    start >> prepare >> validate >> trigger >> finish

dag_triggering_another_dag()
