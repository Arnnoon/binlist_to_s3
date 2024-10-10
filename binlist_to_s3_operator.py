import time
import requests
import json
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv
from airflow.models import BaseOperator
from botocore.exceptions import NoCredentialsError

load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')

class BinListToS3Operator(BaseOperator):

    def __init__(self, s3_bucket, max_retries=3, *args, **kwargs):
        super(BinListToS3Operator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.max_retries = max_retries

    def get_binlist(self):
        return ['45717360', '45717360']

    def fetch_binlist_data(self, bin_number):
        url = f"https://lookup.binlist.net/{bin_number}"
        headers = {"Accept-Version": "3"}
        retries = 0
        while retries < self.max_retries:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                self.log.warning("Rate limit exceeded. Retrying after 60 seconds...")
                time.sleep(60)
                retries += 1
            elif response.status_code == 404:
                self.log.warning(f"No matching card found for BIN: {bin_number}.")
                return None
            else:
                raise Exception(f"Error fetching data from BinList: {response.status_code}")
        raise Exception("Max retries exceeded. Failed to fetch BIN list data.")

    def upload_to_s3(self, data, s3_bucket, s3_key):
        s3 = boto3.client('s3')
        try:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=json.dumps(data))
            self.log.info(f"Successfully uploaded data to S3: {s3_bucket}/{s3_key}")
        except NoCredentialsError:
            self.log.error("AWS credentials not available.")
            raise

    def execute(self):
        bins = self.get_binlist()

        for b in bins:
            self.log.info(f"Fetching BIN List data for {b}")
            data = self.fetch_binlist_data(b)
            
            if data:
                self.log.info(f"Uploading data to S3 bucket: {self.s3_bucket}, key: {b}")
                self.upload_to_s3(data, self.s3_bucket, b)
            else:
                self.log.warning(f"Failed to fetch data for BIN number: {b}. Skipping upload.")


# ---------------------------------------------------------------------------------------------------------------------

op = BinListToS3Operator(
    task_id=f'binlist_to_s3_task_{datetime.now().strftime("%Y%m%d")}',
    s3_bucket='opntest-s3-bucket',
)

op.execute()
