# data_extraction.py
import pandas as pd
import requests
import boto3
import csv
import json
import tabula
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def extract_from_csv(self, file_path):
        with open(file_path, 'r') as file:
            data = csv.reader(file)
            return list(data)
    
    def extract_legacy_users(self):
        return pd.read_sql_table('legacy_users', self.db_connector.engine)

    def extract_from_api(self, api_url):
        response = requests.get(api_url)
        return json.loads(response.content)

    #def extract_from_s3(self, s3_uri):
       # s3_client = boto3.client('s3')
       # bucket_name = s3_uri.split('/')[2]
       # object_key = '/'.join(s3_uri.split('/')[3:])
       # obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        #if object_key.endswith('.csv'):
         #   return pd.read_csv(obj['Body'])
       # elif object_key.endswith('.json'):
        #    return pd.read_json(obj['Body'].read().decode('utf-8'))
       # else:
          #  raise ValueError('Unsupported file format')

    def extract_from_s3(self, s3_uri):
            if s3_uri.endswith('.csv'):
                return pd.read_csv(s3_uri)
            elif s3_uri.endswith('.json'):
                return pd.read_json(s3_uri)
            else:
                raise ValueError('Unsupported file format')

    def extract_from_json(self, file_path):
            with open(file_path, 'r') as file:
                return json.load(file)

    def read_rds_table(self, table_name):
        # Pass credentials to init_db_engine
        engine = self.db_connector.init_db_engine(self.db_connector.creds)
        return pd.read_sql_table(table_name, engine)

    #def read_rds_table(self, table_name):
       # return pd.read_sql_table(table_name, self.db_connector.init_db_engine())

    def retrieve_pdf_data(self, file_path):
        data_frames = tabula.read_pdf(file_path, pages='all', multiple_tables=True)
        return pd.concat(data_frames, ignore_index=True)

    def list_number_of_stores(self, endpoint, headers):
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()  # Raise an error for bad HTTP status codes

            # Assuming the API returns a numeric value or an error message
            num_stores = response.json()
            if isinstance(num_stores, int):
                return num_stores
            else:
                print(f"Error in API response: {num_stores}")
                return 0  # Or handle the error as appropriate

        except requests.RequestException as e:
            print(f"HTTP Request failed: {e}")
            return 0  # Or handle the error as appropriate

    def retrieve_stores_data(self, endpoint, headers):
        try:
            stores = []
            for store_number in range(0, 451):  # 450 stores
                store_endpoint = f"{endpoint}/{store_number}"
                response = requests.get(store_endpoint, headers=headers)
                response.raise_for_status()  # Check for HTTP errors for each request
                stores.append(response.json())
            return pd.DataFrame(stores)
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return pd.DataFrame()  # Or handle this scenario as needed
        except Exception as err:
            print(f"Other error occurred: {err}")
            return pd.DataFrame()  # Or handle this scenario as needed

    

    def list_db_tables(self):
        # Use Inspector to get table names
        insp = reflection.Inspector.from_engine(self.db_connector.engine)
        return insp.get_table_names()

     