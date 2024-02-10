import logging
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator


def run_query(query: str,
              client: bigquery.Client) -> RowIterator:
    """
    Executes a BigQuery query and returns the results.

    Args:
        query (str): The SQL query to execute.
        client (bigquery.Client): The BigQuery client for executing the query.

    Returns:
        bigquery.table.RowIterator: The results of the query.
    """

    # API request - dry run query to estimate costs
    query_job = client.query(query)

    # Execute the query
    return query_job.result()


def calculate_query_size(query: str,
                         client: bigquery.Client,
                         job_config: bigquery.QueryJobConfig) -> float:
    """
    Calculates the size of the data that a BigQuery query will process.

    Args:
        query (str): The SQL query to execute.
        client (bigquery.Client): The BigQuery client for executing the query.
        job_config (bigquery.QueryJobConfig): The configuration for the query job.

    Returns:
        float: The size of the data that the query will process, in gigabytes, rounded to 2 decimal places.
    """

    # API request - dry run query to estimate costs
    query_job = client.query(query, job_config=job_config)

    # convert to megabytes and round to 2 decimal places
    return round(query_job.total_bytes_processed / 1024**2, 2)


def delete_dataset_tables(dataset_id: str, client: bigquery.Client) -> None:
    """
    Deletes all tables in a BigQuery dataset.

    Args:
        dataset_id (str): The ID of the dataset.
        client (bigquery.Client): The BigQuery client for executing the query.

    Returns:
        None
    """

    # Get a list of all tables in the dataset
    tables = list(client.list_tables(dataset_id))

    # Delete each table
    for table in tables:
        client.delete_table(table, not_found_ok=True)


def log_progress(message):
    # Configure the logging module to write output to a file
    logging.basicConfig(filename='./etl_log.txt', level=logging.INFO,
                        format='%(asctime)s : %(message)s',
                        datefmt='%Y-%h-%d-%H:%M:%S')

    # Log the message
    logging.info(message)
