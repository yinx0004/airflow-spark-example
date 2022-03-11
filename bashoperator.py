from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'bash_operator_spark_submit',
    default_args = {
        "depends_on_past": False,
        "retries": 0,
    },
    start_date=datetime(2022, 3, 11),
    description="A simple bashoperator run spark-submit  tutorial DAG",
    schedule_interval="@daily",
    catchup=False,
    tags=['test'],
)

submit_task = BashOperator(
    task_id="simple_print",
    bash_command="spark-submit --master spark://203.116.180.76:7077 --name arrow-spark test.py",
    dag=dag,
)
