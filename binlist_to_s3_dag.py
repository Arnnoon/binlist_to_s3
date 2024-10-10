import os
from datetime import datetime
from dotenv import load_dotenv
from pendulum import today
from airflow import DAG
from binlist_to_s3_operator import BinListToS3Operator

load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')

default_args = {
    'owner': 'airflow',
    'start_date': today('UTC'),
}

with DAG(
    'binlist_to_s3_dag',
    default_args=default_args,
    schedule='0 14 * * *',
    catchup=False,
) as dag:

    binlist_to_s3 = BinListToS3Operator(
        task_id=f'binlist_to_s3_task_{datetime.now().strftime("%Y%m%d")}',
        s3_bucket=S3_BUCKET,
    )

    binlist_to_s3
