from airflow.decorators import dag, task

@task()
def receive_data():
    print("Downstream DAG: Receiving data from upstream DAG...")

@task()
def process_data():
    print("Downstream DAG: Processing data...")

@task()
def store_results():
    print("Downstream DAG: Storing results... Done!")

@dag(
    dag_id="dag_triggered_by_another",
    description="A simple downstream DAG triggered by another DAG (Astronomer Airflow 3 style)",
    catchup=False,
    tags=["demo", "triggered", "downstream"],
)

def dag_triggered_by_another():
    r = receive_data()
    p = process_data()
    s = store_results()

    r >> p >> s

dag_triggered_by_another()
