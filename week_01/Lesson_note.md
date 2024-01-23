## Docker Lesson Note

- Open a python image and specify entrypoint as bash to be able to install packages 
```bash
docker run -it --entrypoint=bash python:3.10
```

- Example of Running postgres locally with docker
```bash
docker run -it \
  -e POSTGRES_USER='user' \
  -e POSTGRES_PASSWORD='password' \
  -e POSTGRES_DB='pg_db' \
  -v local_volume_dir:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:14
```

- Running pgAdmin locally with docker
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL='admin@admin.com' \
  -e PGADMIN_DEFAULT_PASSWORD='root' \
  -p 8080:80 \
  dpage/pgadmin4
```

- Create a docker network
```bash
docker network create my_network
```

- Running the postgres and pgAdmin containers on the same network
```bash
  docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:14
```

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL='admin@admin.com' \
  -e PGADMIN_DEFAULT_PASSWORD='root' \
  -p 8080:80 \
  --network zoomcamp-pg-network \
  --name pgadmin \
  dpage/pgadmin4
```


- Run the dockerized ingest script after running Postgres with `docker-compose`
```bash
docker run -it \
    --network=zoomcamp-data-engineer-2024_default \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --db_table=taxi_trips \
    --parquet_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
```