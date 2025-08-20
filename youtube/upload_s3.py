import boto3
import os
from dotenv import load_dotenv
import re

# Load environment variables from .env
load_dotenv()

# Get values from environment
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bucket_name = os.getenv("BUCKET_NAME")
local_folder = os.getenv("LOCAL_FILE")

# Initialize S3 client with credentials
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region,
)

# Regex to capture region code from CSV filenames like USvideos.csv
region_pattern = re.compile(r"([A-Z]{2})videos\.csv", re.IGNORECASE)

# Loop through all files in the folder
for root, dirs, files in os.walk(local_folder):
    for file in files:
        local_path = os.path.join(root, file)

        if file.endswith(".json"):
            # Preserve relative folder structure inside S3
            s3_key = os.path.relpath(local_path, local_folder)
            s3.upload_file(local_path, bucket_name, f"youtube/{s3_key}")
            print(
                f"✅ Uploaded JSON: {local_path} → s3://{bucket_name}/youtube/{s3_key}"
            )

        elif file.endswith(".csv"):
            # Extract region code (e.g. "US" from "USvideos.csv")
            match = region_pattern.match(file)
            if match:
                region_code = match.group(1).lower()  # make region lowercase
                s3_key = f"raw_statistics/region={region_code}/{file}"
                s3.upload_file(local_path, bucket_name, s3_key)
                print(f"✅ Uploaded CSV: {local_path} → s3://{bucket_name}/{s3_key}")
            else:
                print(f"⚠️ Skipped CSV (no region match): {file}")
