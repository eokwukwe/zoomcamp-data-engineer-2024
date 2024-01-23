FROM python:3.10

# Install dependencies
RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet

WORKDIR /app
COPY ./week_01/ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]