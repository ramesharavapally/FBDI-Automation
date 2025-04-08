import logging
import boto3
from botocore.exceptions import ClientError
import os
import datetime
from dotenv import load_dotenv

load_dotenv(override=True)


class S3LoggerHandler(logging.Handler):
    def __init__(self, bucket_name, log_file_prefix, aws_access_key=None, aws_secret_key=None, region_name=None, buffer_size=10):
        super().__init__()
        self.bucket_name = bucket_name
        self.log_file_prefix = log_file_prefix
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
        self.log_buffer = []
        self.buffer_size = buffer_size  # Number of log entries before uploading

    def emit(self, record):
        print("Emitting log record...")        
        try:
            log_entry = self.format(record)
            self.log_buffer.append(log_entry)

            # Automatically upload logs if buffer size is reached
            if len(self.log_buffer) >= self.buffer_size:
                print("Buffer size reached, uploading logs...")
                self.upload_logs()
        except Exception as e:
            print(f"Failed to emit log record: {e}")

    def flush(self):
        """Flush the log buffer by uploading remaining logs to S3."""
        print("Flushing log buffer...")
        try:
            self.upload_logs()
        except Exception as e:
            print(f"Failed to flush log buffer: {e}")

    def upload_logs(self):
        try:
            if not self.log_buffer:
                return
            # print("Uploading logs to S3...")
            # print(self.log_buffer)

            self.ensure_bucket_exists()  # Ensure the bucket exists before uploading

            log_content = "\n".join(self.log_buffer)
            self.log_buffer = []  # Clear the buffer after uploading

            timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d')
            log_file_name = f"{self.log_file_prefix}_{timestamp}.log"

            # Check if the log file already exists in the bucket
            try:
                existing_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=log_file_name)
                existing_content = existing_object['Body'].read().decode('utf-8')
                log_content = existing_content + "\n" + log_content
            except self.s3_client.exceptions.NoSuchKey:
                # Log file does not exist, proceed with new content
                pass

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=log_file_name,
                Body=log_content
            )
        except ClientError as e:
            print(f"Failed to upload logs to S3: {e}")

    def ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:  # Bucket does not exist
                create_bucket_params = {'Bucket': self.bucket_name}

                # Add LocationConstraint only if the region is not the default (us-east-1)
                if self.s3_client.meta.region_name != 'us-east-1':
                    create_bucket_params['CreateBucketConfiguration'] = {
                        'LocationConstraint': self.s3_client.meta.region_name
                    }

                self.s3_client.create_bucket(**create_bucket_params)