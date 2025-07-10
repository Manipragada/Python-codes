from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import boto3

BUCKET_NAME = 'your-s3-bucket'
WHEEL_DEST_PATH = 'your/path/package.whl'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'emr_ec2_build_whl_upload',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

✅ 3. Step 1: Create EMR Cluster on EC2

JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-EMR-Cluster",
    "ReleaseLabel": "emr-6.9.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
}

create_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    dag=dag,
)
✅ 4. Step 2: Build .whl File
Ensure setup.py is in the project root.

def build_whl_file():
    subprocess.run(["python3", "setup.py", "bdist_wheel"], check=True)

build_package = PythonOperator(
    task_id='build_python_whl',
    python_callable=build_whl_file,
    dag=dag,
)
✅ 5. Step 3: Upload .whl to S3

def upload_to_s3():
    s3 = boto3.client('s3')
    with open('dist/your_package.whl', 'rb') as data:
        s3.upload_fileobj(data, BUCKET_NAME, WHEEL_DEST_PATH)

upload_whl = PythonOperator(
    task_id='upload_whl_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)
✅ 6. Optional: Terminate EMR cluster

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)
✅ 7. DAG Task Flow

create_cluster >> build_package >> upload_whl >> terminate_cluster