from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
with DAG(
    dag_id="a_simple_dag_example",
    start_date=pendulum.datetime(2025, 7, 31, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # Task 1: A simple bash command
    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo 'This is Task A running!'",
    )

    # Task 2: Another bash command
    task_b = BashOperator(
        task_id="task_b",
        bash_command="echo 'This is Task B, running after A!'",
    )

    # Task 3: A final bash command
    task_c = BashOperator(
        task_id="task_c",
        bash_command="echo 'All done! This was Task C.'",
    )

    # Set the task dependencies (the order of execution)
    task_a >> task_b >> task_c
