from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="01_rockets",
    start_date=datetime(year=2025, month=3, day=1),
    end_date=datetime(year=2026, month=3, day=1),
    schedule="@daily",
):

    procure_rocket_material_task = EmptyOperator(task_id="procure_rocket_material")
    procure_fuel_task = EmptyOperator(task_id="procure_fuel")
    build_stage_tasks = [EmptyOperator(task_id=f"build_stage_{i}") for i in range(1, 4)]
    launch_task = EmptyOperator(task_id="launch")

    procure_rocket_material_task >> build_stage_tasks
    procure_fuel_task >> build_stage_tasks[2]
    build_stage_tasks >> launch_task