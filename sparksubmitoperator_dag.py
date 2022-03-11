from datetime import datetime
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='test_spark_submit_operator',
    schedule_interval=None,
    start_date=datetime(2022, 3, 11),
    catchup=False,
    tags=['test'],
) as dag:
    # [START howto_operator_spark_submit]
    submit_job = SparkSubmitOperator(
    conn_id='spark_local',
    application="test.py", task_id="submit_job"
    )
    # [END howto_operator_spark_submit]
