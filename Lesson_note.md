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
  -e POSTGRES_USER='user' \
  -e POSTGRES_PASSWORD='password' \
  -e POSTGRES_DB='pg_db' \
  -v $(pwd)ny_taxi_postgres_data:/var/lib/postgresql/data \
  --network zoomcamp-pg-network \
  --name zoomcamp-pg-database \
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