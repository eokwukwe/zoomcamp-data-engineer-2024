import os
import time
from google.cloud import bigquery, storage

import gcs_utils
import queries as q
import bigquery_utils


def countdown(seconds, message: str = None):
    print(message if message else '')

    for i in range(seconds):
        print(i+1, end=' ', flush=True)
        time.sleep(1)


if __name__ == "__main__":

    # Create BigQuery client
    client = bigquery.Client()

    # Create GCS client
    gcs_client = storage.Client()

    # Create a QueryJobConfig object to estimate costs
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    try:

        # upload the green taxi data to GCS
        msg = f">>>>> Uploading green taxi data to GCS bucket: {q.bucket_name}\n"
        print(msg)
        bigquery_utils.log_progress(msg)

        gcs_utils.web_to_gcs(client=gcs_client,
                             bucket_name=q.bucket_name,
                             year='2022',
                             service='green')

        time.sleep(4)

        # # Create external table from GCS
        msg = f">>>>> Creating external table from GCS bucket: {q.bucket_name}\n"
        print(msg)
        bigquery_utils.log_progress(msg)
        bigquery_utils.run_query(q.create_external_table, client)

        time.sleep(4)

        # Create a non-partitioned table from external table
        print(">>>>> Creating non-partitioned table from external table")
        bigquery_utils.log_progress(
            "Creating non-partitioned table from external table")

        bigquery_utils.run_query(q.create_non_partitioned_table, client)

        print(f">>>>> Non-partitioned table created\n")
        bigquery_utils.log_progress("Non-partitioned table created\n")

        time.sleep(4)

        # count of records for the 2022 Green Taxi Data
        print(">>>>> Querying count of records for the 2022 Green Taxi Data")
        bigquery_utils.log_progress(
            "Querying count of records for the 2022 Green Taxi Data")

        result = bigquery_utils.run_query(q.count_record, client)

        count = [row[0] for row in result][0]

        print(
            f">>>>> Count of records for the 2022 Green Taxi Data: {count}\n")
        bigquery_utils.log_progress(
            f"Count of records for the 2022 Green Taxi Data: {count}\n")

        time.sleep(4)

        # Estimated amount of data that will be read when querying each table
        # for the distinct number of PULocationIDs
        print(">>>>> Estimating amount of data the distinct number of PULocationIDs")
        bigquery_utils.log_progress(
            "Estimating amount of data the distinct number of PULocationIDs")

        tbl_size = bigquery_utils.calculate_query_size(
            q.distinct_pulocation_table, client, job_config)
        extbl_size = bigquery_utils.calculate_query_size(
            q.distinct_pulocation_external, client, job_config)

        print(f">>>>> BigQuery Table size: {tbl_size}MB")
        print(f">>>>> External Table size: {extbl_size}MB\n")
        bigquery_utils.log_progress(
            f"BigQuery Table size: {tbl_size}MB::::"
            f"External Table size: {extbl_size}MB\n")

        time.sleep(4)

        # How many records have a fare_amount of 0?
        print(">>>>> Querying how many records have a fare_amount of 0")
        bigquery_utils.log_progress(
            "Querying how many records have a fare_amount of 0")

        result = bigquery_utils.run_query(q.zero_fare_amount, client)
        num_records = [row[0] for row in result][0]

        print(
            f">>>>> Number of records with a fare_amount of 0: {num_records}\n")
        bigquery_utils.log_progress(
            f"Number of records with a fare_amount of 0: {num_records}\n")

        time.sleep(4)

        # Create a new table Partition by lpep_pickup_datetime & Cluster by
        # PUlocationID
        print(">>>>> Creating a new table Partition by lpep_pickup_datetime & Cluster by PUlocationID")
        bigquery_utils.log_progress(
            "Creating a new table Partition by lpep_pickup_datetime & Cluster by PUlocationID")

        bigquery_utils.run_query(q.create_partitioned_table, client)

        print(f">>>>> Partitioned table created\n")
        bigquery_utils.log_progress("Partitioned table created\n")

        time.sleep(4)

        # Estimate the amount of data that will be read when querying the
        # distinct PULocationID between lpep_pickup_datetime 06/01/2022 and
        # 06/30/2022 for the non-partitioned and partitioned tables
        print(">>>>> Estimating amount of data the distinct number of PULocationIDs between lpep_pickup_datetime 06/01/2022 and 06/30/2022")
        bigquery_utils.log_progress(
            "Estimating amount of data the distinct number of PULocationIDs between lpep_pickup_datetime 06/01/2022 and 06/30/2022\n")

        non_partitioned_size = bigquery_utils.calculate_query_size(
            q.distinct_pulocation_range, client, job_config)
        partitioned_size = bigquery_utils.calculate_query_size(
            q.distinct_pulocation_range_partitioned, client, job_config)

        print(f">>>>> Non-partitioned Table size: {non_partitioned_size}MB")
        print(f">>>>> Partitioned Table size: {partitioned_size}MB")

        bigquery_utils.log_progress(
            f"Non-partitioned Table size: {non_partitioned_size}MB::::"
            f"Partitioned Table size: {partitioned_size}MB\n")

        # Delete all blobs in the GCS bucket and all tables in the BigQuery dataset
        print(
            f">>>>> All blobs in the bucket {q.bucket_name} and all tables in the dataset {q.dataset} will be deleted after 15 seconds. Counting:")
        bigquery_utils.log_progress(
            f'All blobs in the bucket {q.bucket_name} and all tables in the dataset {q.dataset} will be deleted after 15 seconds.\n')

        countdown(15)

        gcs_utils.delete_bucket_blobs(
            client=gcs_client, bucket_name=q.bucket_name)
        bigquery_utils.delete_dataset_tables(q.dataset, client)

        print(
            f"\n>>>>> All blobs in the bucket {q.bucket_name} and all tables in the dataset {q.dataset} has been deleted.\n")
        bigquery_utils.log_progress(
            f"All blobs in the bucket {q.bucket_name} and all tables in the dataset {q.dataset} has been deleted.\n")

    except Exception as e:
        print(e)
