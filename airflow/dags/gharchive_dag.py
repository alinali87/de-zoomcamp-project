import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
REGION = os.environ.get("GCP_REGION")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME")

URL_TEMPLATE = "https://data.gharchive.org" + "/{{ execution_date.strftime('%Y-%m-%d') }}-{0..23}.json.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output-{{ execution_date.strftime('%Y-%m-%d') }}.json.gz"
GCS_PATH_TEMPLATE = "raw/gh_archive/" + \
    "{{ execution_date.strftime('%Y') }}/" + \
    "{{ execution_date.strftime('%m') }}/" + \
    "{{ execution_date.strftime('%d') }}/" + \
    "{{ execution_date.strftime('%Y-%m-%d') }}.json.gz"
SPARK_JOB_PATH = "dataproc/spark_job.py"


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
}


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

    create_cluster_task = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    )

    copy_spark_job_task = LocalFilesystemToGCSOperator(
        task_id="spark_job_to_gcs",
        src=f"{AIRFLOW_HOME}/{SPARK_JOB_PATH}",
        dst=SPARK_JOB_PATH,
        bucket=BUCKET_NAME,
    )

    processing_task = DataProcPySparkOperator(
        task_id="batch_processing_with_dataproc",
        job_name="pyspark_job_{{ execution_date.strftime('%Y-%m-%d') }}",
        cluster_name=CLUSTER_NAME,
        dataproc_jars=["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
        gcp_conn_id="google_cloud_default",
        region=REGION,
        main=f"gs://{BUCKET_NAME}/{SPARK_JOB_PATH}",
        arguments=[
            "--input_file", f"gs://{BUCKET_NAME}/{GCS_PATH_TEMPLATE}",
            "--general_activity", f"{DATASET_NAME}.general_activity",
            "--active_users", f"{DATASET_NAME}.active_users",
            "--gcs_bucket", BUCKET_NAME,
        ]
    )

    delete_cluster_task = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    download_task >> upload_task >> delete_task >> create_cluster_task >> copy_spark_job_task >> processing_task >> delete_cluster_task
