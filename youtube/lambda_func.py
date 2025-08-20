import os
import json
import urllib.parse
import boto3
import pandas as pd
import awswrangler as wr

# Environment variables
os_input_s3_cleansed_layer = os.environ["s3_cleansed_layer"]
os_input_glue_catalog_db_name = os.environ["glue_catalog_db_name"]
os_input_glue_catalog_table_name = os.environ["glue_catalog_table_name"]
os_input_write_data_operation = os.environ["write_data_operation"]  # overwrite / append


def lambda_handler(event, context):
    # Get S3 bucket and key from event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print(f"Processing s3://{bucket}/{key}")

    s3 = boto3.client("s3")

    try:
        # Download JSON content
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_content = obj["Body"].read().decode("utf-8").strip()

        if not raw_content:
            print("File is empty.")
            return {"message": "Empty file, nothing to process."}

        # Load JSON as dict
        json_data = json.loads(raw_content)

        # Flatten 'items' key
        if "items" in json_data:
            df_flat = pd.json_normalize(json_data["items"], sep=".")
        else:
            print("No 'items' key found in JSON.")
            return {"message": "No data to write."}

        if df_flat.empty:
            print("No data found in 'items'.")
            return {"message": "No data to write."}

        print(
            f"Flattened DataFrame. Columns: {df_flat.columns.tolist()}, Rows: {len(df_flat)}"
        )

        # Ensure S3 path ends with '/'
        s3_path = os_input_s3_cleansed_layer
        if not s3_path.endswith("/"):
            s3_path += "/"

        # Ensure Glue database exists
        wr.catalog.create_database(name=os_input_glue_catalog_db_name, exist_ok=True)

        # Write Parquet to S3 & register/update Glue table
        wr_response = wr.s3.to_parquet(
            df=df_flat,
            path=s3_path,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation.lower(),
            partition_cols=None,
        )

        print(f"Successfully written Parquet to {s3_path}")
        return wr_response

    except Exception as e:
        print(f"Error processing s3://{bucket}/{key}: {str(e)}")
        raise e
