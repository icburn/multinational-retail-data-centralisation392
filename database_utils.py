# database_utils.py
from urllib.parse import quote_plus
import yaml
import sqlalchemy
import pandas as pd

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml', local_creds_file='local_creds_file.yaml'):
        # Read-only database credentials
        self.creds = self.read_db_creds(creds_file)

        # Writable database credentials
        self.local_creds = self.read_db_creds(local_creds_file)

        # Engine for the read-only database
        self.engine = self.init_db_engine(self.creds)

        # Engine for the writable database
        self.local_engine = self.init_db_engine(self.local_creds, local=True)

    @staticmethod
    def read_db_creds(creds_file):
        with open(creds_file, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self, creds, local=False):
        if local:
            # URL-encode the local password
            encoded_password = quote_plus(creds['LOCAL_PASSWORD'])
            engine_url = f"postgresql://{creds['LOCAL_USER']}:{encoded_password}@localhost:5432/sales_data"
        else:
            # No URL encoding needed for the remote password
            engine_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        return sqlalchemy.create_engine(engine_url)

    def list_db_tables(self, local=False):
        engine = self.local_engine if local else self.engine
        return engine.table_names()

    def upload_to_db(self, df, table_name, local=False):
        # Use the writable database engine for uploads
        engine = self.local_engine if local else self.engine
        df.to_sql(table_name, engine, if_exists='replace', index=False)


