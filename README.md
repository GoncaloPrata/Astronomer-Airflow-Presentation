# Astronomer Airflow Presentation

Welcome to the Astronomer Airflow Introduction Project!
This repository is designed to help data engineers get started with Apache Airflow, a powerful platform for authoring, scheduling, and monitoring workflows.

The project leverages Astronomer to make Airflow development seamless, providing a local environment to explore the concepts of DAGs, tasks, operators, and advanced orchestration features.

## Project Goals

Introduce core Airflow concepts (DAGs, tasks, operators).

Demonstrate task dependencies and data passing with XComs.

Explore control flow with trigger rules and branching.

Provide a practical playground for hands-on learning.

## Prerequisites

Before starting, make sure you have the following installed:

- Podman
- Astronomer CLI

## Getting Started

Clone the repository:

``git clone https://github.com/your-username/astronomer-airflow-intro.git``

``cd astronomer-airflow-intro``

Start the Airflow environment with Astronomer:

``astro dev start``

Access the Airflow UI at http://localhost:8080.

## Key Airflow Concepts Covered

This project includes examples and references to essential Airflow functionalities:

- [Operators](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html) : Building blocks of tasks (e.g., PythonOperator, BashOperator, DummyOperator);
- [Dependencies](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#dependencies) : Defining the order of execution with ">>" and "<<";
- [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html) : Sharing data between tasks;
- [Trigger Rules](https://airflow.apache.org/docs/apache-airflow/1.10.9/concepts.html#trigger-rules) : Controlling when a task runs based on upstream task states.
- [Branching](https://www.astronomer.io/docs/learn/airflow-branch-operator) : Creating conditional workflows with BranchPythonOperator.

## Suggested Learning Path

1. Explore the DAGs in the dags/ folder.
2. Review how operators are used to define tasks.
3. Inspect dependencies between tasks using >> and <<.
4. Experiment with XComs to pass data.
5. Modify DAGs to use different trigger rules.
6. Add branching paths for conditional execution.
