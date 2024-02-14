import pprint
import dlt
from dlt.sources.helpers import requests


@dlt.source
def weatherapi_source(api_secret_key=dlt.secrets.value):
    return weatherapi_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {api_secret_key}"}
    return headers


@dlt.resource(write_disposition="append")
def weatherapi_resource(api_secret_key=dlt.secrets.value):
    # headers = _create_auth_headers(api_secret_key)
    url = "https://api.weatherapi.com/v1/current.json"
    params = {
        "q": "NYC",
        "key": api_secret_key
    }

    # make an api call here
    response = requests.get(url, params=params)
    response.raise_for_status()
    yield response.json()


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='weatherapi', destination='duckdb', dataset_name='weatherapi_data'
    )

    # print credentials by running the resource
    data = list(weatherapi_resource())

    # print the data yielded from resource
    pprint.pprint(data)
    # exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(weatherapi_source())

    # pretty print the information on data that was loaded
    print(load_info)
