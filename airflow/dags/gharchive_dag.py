import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
REGION = os.environ.get("GCP_REGION")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME")

URL_TEMPLATE = "https://data.gharchive.org/" + "{{ execution_date.strftime('%Y-%m-%d') }}-{0..23}.json.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output-{{ execution_date.strftime('%Y-%m-%d') }}.json.gz"
GCS_PATH_TEMPLATE = "raw/gh_archive/" + \
    "{{ execution_date.strftime('%Y') }}/" + \
    "{{ execution_date.strftime('%Y-%m') }}/" + \
    "{{ execution_date.strftime('%Y-%m-%d') }}.json.gz"
PYSPARK_JOB = f"{AIRFLOW_HOME}/dataproc/spark_job.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="test_gharchive_dag",  # TODO: "gharchive_dag"
    description="Pipeline for Data Engineering Zoomcamp Project",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2023, 4, 1),
    max_active_runs=1,
    catchup=True
) as dag:

    download_task = BashOperator(
        task_id="download_gharchive_dataset",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    upload_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=OUTPUT_FILE_TEMPLATE,
        dst=GCS_PATH_TEMPLATE,
        bucket=BUCKET_NAME,
    )

    delete_task = BashOperator(
        task_id="delete_dataset_from_local",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE}"
    )

    processing_task = DataProcPySparkOperator(
        task_id="batch_processing_with_dataproc",
        job_name="pyspark_job_{{ execution_date.strftime('%Y-%m-%d') }}",
        cluster_name=f"{CLUSTER_NAME}",
        dataproc_jars=["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
        gcp_conn_id="google_cloud_default",
        region=f"{REGION}",
        main=f"gs://{BUCKET_NAME}/dataproc/spark_job.py",
        arguments=[
            "--input_file", f"gs://{BUCKET_NAME}/{GCS_PATH_TEMPLATE}",
            "--general_activity", f"{DATASET_NAME}.general_activity",
            "--active_users", f"{DATASET_NAME}.active_users"
        ]
    )

    download_task >> upload_task >> delete_task >> processing_task
