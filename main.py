import abc
import io
import json
import re
from os import getenv
import functions_framework
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pyarrow
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
        self._endpoint = "/" + re.findall(r'[A-Z][a-z]*', self.__class__.__name__)[0].lower()

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
        if params is not None:
            self._params.update(params)
        print(self._params)
        response = requests.get(self._api_url + self._endpoint, headers=self._headers, params=self._params)
        response.raise_for_status()
        return response

    def _process_response(self, response):
        self._data = response.json()

    def __create_dataset(self) -> bigquery.Dataset:
        dataset_ref = self._client.dataset(self._dataset_name)

        try:
            dataset = self._client.get_dataset(dataset_ref)
            print('Dataset {} already exists.'.format(dataset))
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'EU'
            dataset = self._client.create_dataset(dataset)
            print('Dataset {} created.'.format(dataset.dataset_id))
        return dataset

    def _create_table(self, table_name: str, schema: List[SchemaField]) -> bigquery.Table:
        dataset_ref = self._client.dataset(self._dataset_name)
        table_ref = dataset_ref.table(table_name)
        try:
            table = self._client.get_table(table_ref)
            print(f'Table {table.table_id} already exists.')
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            table = self._client.create_table(table)
            print(f'Table {table.table_id} created.')

        return table

    def _load_data_to_table(self, table_name: str, df: pd.DataFrame):
        try:
            table_id = f"{self._dataset_name}.{table_name}"
            df.to_gbq(table_id, project_id='holy-water-408717', if_exists='append', progress_bar=True)
        except Exception as e:
            print(f"Error writing: {e}")

    @abc.abstractmethod
    def get_data_and_update_table(self):
        pass

    @staticmethod
    def check_first_run() -> bool:
        storage_client = storage.Client()
        try:
            storage_client.get_bucket('first-run-holy-water-checker')
            return False
        except NotFound:
            print("No bucket. Creating bucket...")
            storage_client.create_bucket('first-run-holy-water-checker')
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
        self._params["date"] = date

        if params is not None:
            self._params.update(params)

        try:
            response = self._make_request(params)
            response.raise_for_status()

            with io.BytesIO(response.content) as buffer:
                parquet_file = pq.ParquetFile(buffer)
                parquet_table = parquet_file.read()
                self._data = parquet_table.to_pandas()

        except requests.exceptions.RequestException as req_exc:
            print(f"Error in request: {req_exc}")

        except pyarrow.ArrowInvalid as arrow_exc:
            print(f"Error in Arrow processing: {arrow_exc}")

        except Exception as e:
            print(f"Unexpected error: {e}")

    def get_data_and_update_table(self):
        df = self._data[
            ["event_time", "transaction_id", "category", "payment_method", "fee", "tax", "iap_item.name",
             "iap_item.price", "discount.amount"]]

        df['iap_item'] = df[['iap_item.name', 'iap_item.price']].apply(lambda row: {'name': row[0], 'price': row[1]},
                                                                       axis=1)
        df = df.drop(columns=["iap_item.name", "iap_item.price"])

        df = df.rename(columns={"discount.amount": "discount_amount"})
        df = df[
            ["event_time", "transaction_id", "category", "payment_method", "fee", "tax", "iap_item", "discount_amount"]
        ]

        self._load_data_to_table(self.__table_name, df)


class InstallsFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "installations"
        self.__schema = [
            bigquery.SchemaField("install_time", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("marketing_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            bigquery.SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("medium", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("campaign", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("keyword", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ad_group", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("landing_page", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sex", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("alpha_2", bigquery.enums.SqlTypeNames.STRING),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def fetch_data(self, date: str, params: dict = None):
        super().fetch_data(date, params)

    def get_data_and_update_table(self):
        records = self._data.get('records', [])
        if isinstance(records, str):
            records = json.loads(records)

        df_columns = ["install_time", "marketing_id", "channel", "medium", "campaign", "keyword",
                      "ad_group", "landing_page", "sex", "alpha_2"]
        df = pd.DataFrame(columns=df_columns)

        for record in records:
            extracted_data = {
                "install_time": record.get("install_time"),
                "marketing_id": record.get("marketing_id"),
                "channel": record.get("channel"),
                "medium": record.get("medium"),
                "campaign": record.get("campaign"),
                "keyword": record.get("keyword"),
                "ad_group": record.get("ad_group"),
                "landing_page": record.get("landing_page"),
                "sex": InstallsFetcher.__categorize_sex(record.get("sex")),
                "alpha_2": record.get("alpha_2"),
            }
            df = df.append(extracted_data, ignore_index=True)

        df['install_time'] = pd.to_datetime(df['install_time'], errors='coerce')

        self._load_data_to_table(self.__table_name, df)

    @staticmethod
    def __categorize_sex(sex):
        if sex and sex[0].lower() == 'm':
            return 'Male'
        elif sex and sex[0].lower() == 'f':
            return 'Female'
        else:
            return 'Other'


class EventsFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self._data = None
        self.__table_name = "user_events"
        self.__schema = [
            bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            bigquery.SchemaField("event_time", bigquery.enums.SqlTypeNames.DATETIME, 'REQUIRED'),
            bigquery.SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("engagement_time_msec", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("user_params", bigquery.enums.SqlTypeNames.RECORD, fields=[
                bigquery.SchemaField("os", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("model", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("model_number", bigquery.enums.SqlTypeNames.INT64),
                bigquery.SchemaField("marketing_id", bigquery.enums.SqlTypeNames.STRING, 'REQUIRED'),
            ]),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def fetch_data(self, date: str, params: dict = None) -> None:
        super().fetch_data(date, params)
        if self._data is None:
            return
        next_page = self._data.get('next_page')
        print(f"Next page: {next_page}")
        return self.fetch_data(date, {'next_page': next_page}) if next_page else None

    def get_data_and_update_table(self):
        data = self._data.get('data', [])
        data = json.loads(data)

        df_columns = ["user_id", "event_time", "event_type", "engagement_time_msec",
                      "user_params"]
        df = pd.DataFrame(columns=df_columns)
        for entity in data:
            if not entity.get("user_params", {}).get("marketing_id"):
                continue

            extracted_data = {
                "user_id": entity.get("user_id"),
                "event_time": pd.to_datetime(entity.get("event_time"), unit='ms', errors='coerce'),
                "event_type": entity.get("event_type"),
                "engagement_time_msec": (
                    pd.to_numeric(entity.get("engagement_time_msec"), errors='coerce')
                    if entity.get("engagement_time_msec") is not None
                    else 0
                ), "user_params": {
                    "os": entity.get("user_params", {}).get("os"),
                    "model": entity.get("user_params", {}).get("model"),
                    "model_number": pd.to_numeric(entity.get("user_params", {}).get("model_number"), errors='coerce'),
                    "marketing_id": entity.get("user_params", {}).get("marketing_id")
                },
            }
            df = df.append(extracted_data, ignore_index=True)

        self._load_data_to_table(self.__table_name, df)

    @staticmethod
    def time_converter(date_time: int):
        timestamp_seconds = date_time / 1000.0
        utc_datetime = datetime.utcfromtimestamp(timestamp_seconds)
        formatted_time = utc_datetime.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_time


class CostsFetcher(DataFetcher):
    def __init__(self, api_url: str, authorization_key: str):
        self.__table_name = "costs"
        self.__schema = [
            bigquery.SchemaField("ad_group", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("landing_page", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("keyword", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("campaign", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("medium", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("location", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ad_content", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("cost", bigquery.enums.SqlTypeNames.FLOAT64),
        ]
        super().__init__(api_url, authorization_key)
        self._create_table(table_name=self.__table_name, schema=self.__schema)

    def fetch_data(self, date: str, params: dict = None):
        self._params["date"] = date

        if params is not None:
            self._params.update(params)

        try:
            response = self._make_request(params)
            response.raise_for_status()

            self._data = response.text

        except requests.exceptions.RequestException as req_exc:
            print(f"Error in request: {req_exc}")

        except Exception as e:
            print(f"Unexpected error: {e}")

    def get_data_and_update_table(self):
        try:
            lines = self._data.strip().split('\n')
            df = pd.read_csv(io.StringIO('\n'.join(lines)), sep='\t')
            df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
            self._load_data_to_table(self.__table_name, df)

        except Exception as e:
            print(f"Error processing data and updating table: {e}")


def get_all_subclass_instances(cls, api_url: str, authorization_key: str) -> List[DataFetcher]:
    subclasses = cls.__subclasses__()
    instances = []
    for subclass in subclasses:
        instance = subclass(api_url, authorization_key)
        instances.append(instance)
        instances.extend(get_all_subclass_instances(subclass, api_url, authorization_key))
    return instances


def first_run(api_url: str, authorization_key: str):
    days = 31
    current_date = datetime.now() - timedelta(days=1)
    for day in range(days, 0, -1):
        previous_date = (current_date - timedelta(days=day)).strftime("%Y-%m-%d")
        instances = get_all_subclass_instances(DataFetcher, api_url, authorization_key)
        shrink_data(instances, previous_date)


def shrink_data(instances: List[DataFetcher], date: str):
    for instance in instances:
        instance.fetch_data(date, params={
            "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"} if isinstance(
            instance, CostsFetcher) else None)
        instance.get_data_and_update_table()


@functions_framework.cloud_event
def main(cloud_event):
    api_url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"
    authorization_key = getenv('AUTH_KEY')
    if DataFetcher.check_first_run():
        first_run(api_url, authorization_key)

    instances = get_all_subclass_instances(DataFetcher, api_url, authorization_key)
    shrink_data(instances, (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))