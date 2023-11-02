from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operator.dataproc import DataprocSubmitPySparkJobOperator
# from airflow.utils.dates import days_ago


# Set the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a dag object
dag = DAG(
    'batch_spark_job',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc cluster',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 2),
    catchup=False,
    tags=['example 2'],
)

# Fetch configuration from Airflow Variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']
BUCKET = config['BUCKET']

# specify pyspark file path
pyspark_job_file_path = f"gs://{BUCKET}/orders_data_process.py"

# Use Jinja template to get configuration from Airflow While triggering the DAG with config
# It checks if execution_date is provided,
# otherwise use the logical date, which is the current date.
# It is useful for the backfilling with late arrival data
# date_variable = "{{ ds_nodash }}"
date_variable = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id="submit_pyspark_job",
    main=pyspark_job_file_path,
    arguments=[f"--date={date_variable}"],  # Passing date as an argument to the PySpark script
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)
