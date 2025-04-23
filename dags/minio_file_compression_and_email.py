from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
import zipfile
import os
from datetime import timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Function to check for new files in MinIO
def check_for_files(**kwargs):
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        bucket_name = "my-bucket"
        prefix = "uploads/"

        # Get the list of processed files from Airflow Variables
        processed_files = Variable.get(
            "processed_files", default_var=[], deserialize_json=True
        )

        logger.info(f"Listing objects in bucket: {bucket_name}, prefix: {prefix}")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        logger.info(f"Raw response from MinIO: {response}")

        if "Contents" not in response:
            logger.info(
                "No 'Contents' key in response. Bucket might be empty or prefix incorrect."
            )
            return False

        files = [
            obj["Key"] for obj in response.get("Contents", []) if obj["Key"] != prefix
        ]
        logger.info(f"Found files: {files}")

        # Filter out already processed files
        new_files = [f for f in files if f not in processed_files]

        if new_files:
            logger.info(f"New files detected: {new_files}")
            kwargs["ti"].xcom_push(key="files", value=new_files)
            return True
        return False
    except Exception as e:
        logger.error(f"Error connecting to MinIO: {str(e)}")
        raise


# Function to compress the file, move it, and gather specifications
def compress_and_get_specs(**kwargs):
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    ti = kwargs["ti"]
    files = ti.xcom_pull(task_ids="wait_for_file", key="files")

    logger.info(f"Files from XCom: {files}")

    if not files:
        raise ValueError("No files detected. Ensure files exist in the bucket.")

    file_key = files[0]  # Process the first new file
    bucket_name = "my-bucket"

    # Download the file
    local_file_path = f"/tmp/{os.path.basename(file_key)}"
    compressed_file_path = f"/tmp/{os.path.basename(file_key)}.zip"

    s3_client.download_file(bucket_name, file_key, local_file_path)

    # Get original file specs
    original_size = os.path.getsize(local_file_path)
    original_specs = (
        f"Original File: {os.path.basename(file_key)}, Size: {original_size} bytes"
    )

    # Compress the file
    with zipfile.ZipFile(compressed_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(local_file_path, os.path.basename(file_key))

    # Get compressed file specs
    compressed_size = os.path.getsize(compressed_file_path)
    compressed_specs = f"Compressed File: {os.path.basename(file_key)}.zip, Size: {compressed_size} bytes"

    # Upload the compressed file to MinIO
    compressed_key = f"compressed/{os.path.basename(file_key)}.zip"
    s3_client.upload_file(compressed_file_path, bucket_name, compressed_key)

    # Move the original file to the 'processed' folder
    processed_key = f"processed/{os.path.basename(file_key)}"
    logger.info(f"Moving file from {file_key} to {processed_key}")
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={"Bucket": bucket_name, "Key": file_key},
        Key=processed_key,
    )

    # Delete the original file from the 'uploads' folder
    logger.info(f"Deleting file from {file_key}")
    s3_client.delete_object(Bucket=bucket_name, Key=file_key)

    # Update the list of processed files in Airflow Variables
    processed_files = Variable.get(
        "processed_files", default_var=[], deserialize_json=True
    )
    processed_files.append(file_key)
    Variable.set("processed_files", processed_files, serialize_json=True)
    logger.info(f"Updated processed files: {processed_files}")

    # Clean up local files
    os.remove(local_file_path)
    os.remove(compressed_file_path)

    return f"{original_specs}\n{compressed_specs}"


# Define the DAG
with DAG(
    "compress_file_on_upload",
    default_args=default_args,
    description="Compress files uploaded to MinIO and send email with specs",
    schedule_interval=timedelta(minutes=1),
    start_date=days_ago(1),
    tags=["file-compression"],
    catchup=False,
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=check_for_files,
        poke_interval=1,  # Check every 1 second for near-instant triggering
        timeout=18 * 60 * 60,
        mode="reschedule",
        dag=dag,
    )

    compress_file = PythonOperator(
        task_id="compress_file",
        python_callable=compress_and_get_specs,
        provide_context=True,
        dag=dag,
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="krishtaltiwari@gmail.com",
        subject="File Compression Report",
        html_content="{{ ti.xcom_pull(task_ids='compress_file') }}",
        dag=dag,
    )

    wait_for_file >> compress_file >> send_email
