# /dags/simple_dag.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
with DAG(
    dag_id="simple_test_dag",
    # This DAG will run once for a date in the past, and then you can trigger it manually.
    # The `start_date` is set to a fixed date in the past to avoid unexpected runs.
    # Using pendulum for timezone-aware datetimes is a best practice.
    start_date=pendulum.datetime(2025, 7, 31, tz="UTC"),
    # This DAG is not set to run on a schedule.
    schedule=None,
    # A good practice is to add tags to organize your DAGs in the Airflow UI.
    tags=["example", "test"],
    # This setting prevents the DAG from running for past, un-run schedules upon deployment.
    catchup=False,
    doc_md="""
    ### Simple Test DAG

    This is a basic DAG to test the Airflow installation and Git-Sync functionality.
    - It has two simple Bash tasks.
    - It does not run on a schedule.
    """,
) as dag:
    # Task 1: A BashOperator to print the current date
    task_1_print_date = BashOperator(
        task_id="print_date",
        bash_command="date",
        doc_md="""
        #### Print Date Task
        This task uses the `date` shell command to print the current date and time
        as seen by the worker that executes it.
        """,
    )

    # Task 2: A BashOperator to print a simple message
    task_2_print_done = BashOperator(
        task_id="print_done",
        bash_command='echo "All tasks are done!"',
        doc_md="""
        #### Print Done Task
        This task simply prints a completion message to the logs.
        """,
    )

    # Define the task dependency.
    # task_1_print_date will run before task_2_print_done.
    task_1_print_date >> task_2_print_done
