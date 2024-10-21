import abc
import io
import json
import re
from os import getenv
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from retrying import retry


class DataFetcher(abc.ABC):
    def __init__(self, api_url: str, authorization_key: str):
        self._data = None
        self._api_url = api_url
        self._dataset_name = "marketing_analytics"
        self._client = bigquery.Client()
        self._params = {"date": ""}
        self._dataset = self.__create_dataset()
        self._headers = {"Authorization": authorization_key}
        self._endpoint = f"/{self.__class__.__name__.split('Fetcher')[0].lower()}"

    @abc.abstractmethod
    def fetch_data(self, date: str, params: dict = None):
        self._params["date"] = date
        try:
            response = self._make_request(params)
            response.raise_for_status()
            self._process_response(response)
        except requests.exceptions.RequestException as req_exc:
            print(f"Error in request: {req_exc}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=3)
    def _make_request(self, params: dict = None) -> requests.Response:
        if params:
            self._params.update(params)
        response = requests.get(self._api_url + self._endpoint, headers=self._headers, params=self._params)
        response.raise_for_status()
        return response

    def _process_response(self, response):
        self._data = response.json()

    def __create_dataset(self) -> bigquery.Dataset:
        dataset_ref = self._client.dataset(self._dataset_name)
        try:
            dataset = self._client.get_dataset(dataset_ref)
            print(f"Dataset {dataset.dataset_id} already exists.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'EU'
            dataset = self._client.create_dataset(dataset)
            print(f"Dataset {dataset.dataset_id} created.")
        return dataset

    def _create_table(self, table_name: str, schema: List[SchemaField]) -> bigquery.Table:
        table_ref = self._client.dataset(self._dataset_name).table(table_name)
        try:
            table = self._client.get_table(table_ref)
            print(f"Table {table.table_id} already exists.")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            table = self._client.create_table(table)
            print(f"Table {table.table_id} created.")
        return table

    def _load_data_to_table(self, table_name: str, df: pd.DataFrame):
        try:
            table_id = f"{self._dataset_name}.{table_name}"
            df.to_gbq(table_id, project_id='holy-water-408717', if_exists='append', progress_bar=False)
        except Exception as e:
            print(f"Error writing data: {e}")

    @abc.abstractmethod
    def get_data_and_update_table(self):
        pass

    @staticmethod
    def check_first_run() -> bool:
        storage_client = storage.Client()
        bucket_name = 'first-run-holy-water-checker'
        try:
            storage_client.get_bucket(bucket_name)
            return False
        except NotFound:
            print(f"Bucket '{bucket_name}' not found. Creating it...")
            storage_client.create_bucket(bucket_name)
            return True


class OrdersFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "orders"
        self.__schema = [
            SchemaField("event_time", bigquery.enums.SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField("transaction_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField("category", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("payment_method", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("fee", bigquery.enums.SqlTypeNames.FLOAT64),
            SchemaField("tax", bigquery.enums.SqlTypeNames.FLOAT64),
            SchemaField("iap_item", bigquery.enums.SqlTypeNames.RECORD, fields=[
                SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
                SchemaField("price", bigquery.enums.SqlTypeNames.FLOAT64)
            ]),
            SchemaField("discount_amount", bigquery.enums.SqlTypeNames.FLOAT64),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def fetch_data(self, date: str, params: dict = None):
        super().fetch_data(date, params)
        try:
            with io.BytesIO(self._data.content) as buffer:
                parquet_table = pq.read_table(buffer)
                self._data = parquet_table.to_pandas()
        except (pyarrow.ArrowInvalid, Exception) as e:
            print(f"Error processing data: {e}")

    def get_data_and_update_table(self):
        df = self._data[
            ["event_time", "transaction_id", "category", "payment_method", "fee", "tax", "iap_item.name", "iap_item.price", "discount.amount"]]
        df['iap_item'] = df[['iap_item.name', 'iap_item.price']].apply(lambda row: {'name': row[0], 'price': row[1]}, axis=1)
        df = df.drop(columns=["iap_item.name", "iap_item.price"]).rename(columns={"discount.amount": "discount_amount"})
        self._load_data_to_table(self.__table_name, df)


class InstallsFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "installations"
        self.__schema = [
            SchemaField("install_time", bigquery.enums.SqlTypeNames.DATETIME),
            SchemaField("marketing_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("medium", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("campaign", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("keyword", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("ad_group", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("landing_page", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("sex", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("alpha_2", bigquery.enums.SqlTypeNames.STRING),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def get_data_and_update_table(self):
        df = pd.DataFrame(self._data.get('records', []))
        df['install_time'] = pd.to_datetime(df['install_time'], errors='coerce')
        self._load_data_to_table(self.__table_name, df)

    @staticmethod
    def __categorize_sex(sex):
        return {'m': 'Male', 'f': 'Female'}.get(sex[0].lower(), 'Other') if sex else 'Other'


class EventsFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "user_events"
        self.__schema = [
            SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField("event_time", bigquery.enums.SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("engagement_time_msec", bigquery.enums.SqlTypeNames.INT64),
            SchemaField("user_params", bigquery.enums.SqlTypeNames.RECORD, fields=[
                SchemaField("os", bigquery.enums.SqlTypeNames.STRING),
                SchemaField("model", bigquery.enums.SqlTypeNames.STRING),
                SchemaField("model_number", bigquery.enums.SqlTypeNames.INT64),
                SchemaField("marketing_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            ]),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def get_data_and_update_table(self):
        df = pd.DataFrame(self._data.get('data', []))
        df['event_time'] = pd.to_datetime(df['event_time'], unit='ms', errors='coerce')
        self._load_data_to_table(self.__table_name, df)


class CostsFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "costs"
        self.__schema = [
            SchemaField("ad_group", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("cost", bigquery.enums.SqlTypeNames.FLOAT64),
            SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("campaign", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("cost_time", bigquery.enums.SqlTypeNames.DATETIME),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def get_data_and_update_table(self):
        df = pd.DataFrame(self._data.get('costs', []))
        df['cost_time'] = pd.to_datetime(df['cost_time'], errors='coerce')
        self._load_data_to_table(self.__table_name, df)


class PingFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "pings"
        self.__schema = [
            SchemaField("app_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField("ping_time", bigquery.enums.SqlTypeNames.DATETIME),
            SchemaField("session_duration", bigquery.enums.SqlTypeNames.FLOAT64),
            SchemaField("device_id", bigquery.enums.SqlTypeNames.STRING),
            SchemaField("os_version", bigquery.enums.SqlTypeNames.STRING),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def get_data_and_update_table(self):
        df = pd.DataFrame(self._data.get('pings', []))
        df['ping_time'] = pd.to_datetime(df['ping_time'], errors='coerce')
        self._load_data_to_table(self.__table_name, df)


class FetchersFactory:
    def __init__(self, api_url: str, authorization_key: str):
        self.api_url = api_url
        self.authorization_key = authorization_key

    def create_fetcher(self, fetcher_type: str) -> DataFetcher:
        fetchers_map = {
            "orders": OrdersFetcher,
            "installs": InstallsFetcher,
            "events": EventsFetcher,
            "costs": CostsFetcher,
            "pings": PingFetcher
        }
        fetcher_class = fetchers_map.get(fetcher_type.lower())
        if fetcher_class:
            return fetcher_class(self.api_url, self.authorization_key)
        raise ValueError(f"Unknown fetcher type: {fetcher_type}")

def main():
    # Setup API URL and Authorization key (usually from environment variables or config)
    api_url = "https://api.example.com/data"  # Replace with the actual API URL
    authorization_key = getenv("API_AUTH_KEY")  # Fetch from environment or replace with actual key

    # Check if it's the first run
    if DataFetcher.check_first_run():
        print("First run detected. Initializing required resources...")

    # Create a fetcher factory
    factory = FetchersFactory(api_url, authorization_key)

    # Define which fetchers to run and for which dates (example: fetching yesterday's data)
    date_to_fetch = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # List of fetcher types we want to run
    fetcher_types = ["orders", "installs", "events", "costs", "pings"]

    # Loop through each fetcher type, fetch data, and update the respective table
    for fetcher_type in fetcher_types:
        try:
            fetcher = factory.create_fetcher(fetcher_type)
            print(f"Fetching {fetcher_type} data for date: {date_to_fetch}")
            fetcher.fetch_data(date_to_fetch)
            fetcher.get_data_and_update_table()
            print(f"{fetcher_type.capitalize()} data successfully updated.")
        except Exception as e:
            print(f"Error processing {fetcher_type}: {e}")


if __name__ == "__main__":
    main()
