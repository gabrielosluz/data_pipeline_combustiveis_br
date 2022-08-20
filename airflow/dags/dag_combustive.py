from airflow import DAG
from airflow.utils.dates import days_ago, datetime
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import json
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False
}

CLUSTER_NAME = 'cluster_name'
REGION='us-central1'
PROJECT_ID='projeto_gcp'
CODE_BUCKET_NAME='bucket_para_codigo_pyspark'
PYSPARK_FILE='main.py'

with DAG(
        dag_id="dag_combustivel",
        default_args=default_args,
        description="Dag de carga de dados dos combustíveis",
        start_date=datetime(2004,1,1),
        schedule_interval="0 0 1 */6 *",
        tags=["combustivel"],
        max_active_runs=1
        #catchup=False
) as dag:
    
    start_dag = DummyOperator(task_id="start_dag")

    get_data = SimpleHttpOperator(
    task_id='get_op',
    method='POST',
    http_conn_id='api_data_challenge',
    endpoint='download_combustivel',
    data=json.dumps({
        "bucket_name": "ac-data-challenge-combustiveis-brasil-gl-raw",
        "url":"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{{ dag_run.logical_date.strftime('%Y') }}-{{ '01' if dag_run.logical_date.month <= 6 else '02'}}.csv",
        "output_file_prefix": "combustiveis-brasil/{{ dag_run.logical_date.strftime('%Y') }}/{{ '01' if dag_run.logical_date.month <= 6 else '02'}}/ca-{{ dag_run.logical_date.strftime('%Y') }}-{{ '01' if dag_run.logical_date.month <= 6 else '02'}}.csv"
        }),
    headers={"Content-Type":"application/json"})

    CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
     },
    }


    create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME)

    PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
    "main_python_file_uri": f"gs://{CODE_BUCKET_NAME}/{PYSPARK_FILE}",
    "jar_file_uris":["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"],
    "args": [
    '--path_input', "gs://bucket_raw/combustiveis-brasil/{{ dag_run.logical_date.strftime('%Y') }}/{{ '01' if dag_run.logical_date.month <= 6 else '02'}}/ca-{{ dag_run.logical_date.strftime('%Y') }}-{{ '01' if dag_run.logical_date.month <= 6 else '02'}}.csv",
    '--path_output',"gs://bucket_curated/combustiveis-brasil/{{ dag_run.logical_date.strftime('%Y') }}/{{ '01' if dag_run.logical_date.month <= 6 else '02'}}/",
    '--file_format','parquet',
    '--bq_dataset','data_challenge',
    '--table_bq','combustiveis_brasil'
    ]}
    }


    submit_job = DataprocSubmitJobOperator(
    task_id="pyspark_task",
    job=PYSPARK_JOB,
    region=REGION, 
    project_id=PROJECT_ID)
    
    delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    )

    fim_dag = DummyOperator(task_id="fim_dag")

    start_dag >> get_data>> create_cluster >> submit_job >> delete_cluster >> fim_dag

