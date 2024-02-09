

dataset = 'zoomcamp-2024-411805.nytaxi_date'
bigquery_table = f'{dataset}.green_taxi_tripdata'
external_table = f'{dataset}.external_green_taxi_tripdata'
partitioned_table = f'{dataset}.green_taxi_tripdata_partitioned'

# Query to count of records for the 2022 Green Taxi Data
count_record = (f'SELECT count(*) FROM `{bigquery_table}`')

# query to count the distinct number of PULocationIDs for the entire
# dataset on both the tables
distinct_pulocation_table = (
    f'SELECT count(DISTINCT PULocationID) FROM `{bigquery_table}`')

distinct_pulocation_external = (
    f'SELECT count(DISTINCT PULocationID) FROM `{external_table}`')

# How many records have a fare_amount of 0?
zero_fare_amount = (
    f'SELECT count(*) FROM `{bigquery_table}` WHERE fare_amount = 0')

# Create a new table Partition by lpep_pickup_datetime Cluster on PUlocationID
create_table = (
    f'''
    CREATE TABLE `{partitioned_table}`
    PARTITION BY DATE(lpep_pickup_datetime)
    CLUSTER BY PULocationID
    AS
    SELECT * FROM `{bigquery_table}`
    '''
)

# Write a query to retrieve the distinct PULocationID between
# lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
distinct_pulocation_range = (
  f'''
  SELECT DISTINCT PULocationID
  FROM `{bigquery_table}`
  WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'
  '''
)

distinct_pulocation_range_partitioned = (
  f'''
  SELECT DISTINCT PULocationID
  FROM `{partitioned_table}`
  WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'
  '''
)


