import os
from google.cloud import bigquery

import utils
import queries as q


# Create BigQuery client
client = bigquery.Client()

# Create a QueryJobConfig object to estimate costs
job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

# gs://zoomcamp-2024-bucket/yellow_tripdata_2021-01.parquet
bucket_name = os.environ.get("GOOGLE_GCS_BUCKET")
bucket_uri = f'gs://{bucket_name}/green_tripdata_2022-*.parquet'


dataset = 'bigquery-public-data.usa_names.usa_1910_2013'
# Perform a query.
# QUERY = (
#     f'SELECT count(*) FROM `{dataset}.usa_1910_2013` '
#     'WHERE state = "TX"')
QUERY = (
    f'SELECT name FROM `{dataset}` '
    'WHERE state = "TX" '
    'LIMIT 10')
# query_job = client.query(QUERY)  # API request
# rows = query_job.result()  # Waits for query to finish

# print("Query results: ", rows)

# for row in rows:
#     print(row)

# Build the query
query = """
    SELECT *
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE year > 2000
"""


print(utils.calculate_query_size(query, client, job_config))
rows = utils.run_query(QUERY, client)

for row in rows:
    print(row[0])
