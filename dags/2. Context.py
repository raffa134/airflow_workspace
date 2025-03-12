from datetime import datetime
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _print_context_info(**context):    
    print(f"This script was executed at " + str(context["execution_date"]))
    print(f"Three days after execution is " + str(context["execution_date"] + timedelta(days=3)))
    print(f"This script run date is " + str(context["ds"]))

with DAG(
    dag_id="02_context",
    start_date=datetime(year=2025, month=3, day=1),
    end_date=datetime(year=2026, month=3, day=1),
    schedule="@daily",
):
    
    print_context_info_bash_task = BashOperator(
        task_id="print_context_info_bash",
        bash_command="echo {{ task.task_id }} is running in the {{ dag.dag_id }} pipeline"
    )

    print_context_info_python_task = PythonOperator(
        task_id="print_context_info_python",
        python_callable=_print_context_info
    )

    print_context_info_bash_task >> print_context_info_python_task