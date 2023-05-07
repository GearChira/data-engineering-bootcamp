# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-csv

import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "civil-icon-384414"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

## For adjusting autodetect, partitiong fields, clustering fields on Big Query ##
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    # time_partitioning=bigquery.TimePartitioning(
    #     type_=bigquery.TimePartitioningType.DAY,
    #     field="created_at",
    # ),
    # clustering_fields=["event_id"],
)

file_path = "data/addresses.csv" ## Change file name under data folder here ##
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_load_data_example.addresses" ## Change the table name appears on Big Query ##
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")