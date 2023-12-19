# main.py
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd

def main():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    data_cleaner = DataCleaning()

   # headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
   # store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
   # number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    df_users = data_extractor.extract_legacy_users()
    #df_card_details = data_extractor.retrieve_pdf_data('card_details.pdf')
   # store_data = data_extractor.retrieve_stores_data(store_endpoint, headers)

    clean_users = data_cleaner.clean_user_data(df_users)
   # clean_card_details = data_cleaner.clean_card_data(df_card_details)
    #clean_store_data = data_cleaner.clean_store_data(store_data)

    db_connector.upload_to_db(clean_users, 'dim_users', local=True)
   # db_connector.upload_to_db(clean_card_details, 'dim_card_details', local=True)
   # db_connector.upload_to_db(clean_store_data, 'dim_store_details', local=True)


     # Extract, clean, and upload product data
  #  product_data_s3_uri = 's3://data-handling-public/products.csv'
  #  product_data = data_extractor.extract_from_s3(product_data_s3_uri)
  #  clean_product_data = data_cleaner.clean_products_data(product_data)
  #  db_connector.upload_to_db(clean_product_data, 'dim_products', local=True)

    # List tables and find orders table
   # tables = data_extractor.list_db_tables()
   # orders_table_name = [table for table in tables if 'orders_table' in table][0]

    # Extract orders data
   # orders_data = data_extractor.read_rds_table(orders_table_name)

    # Clean orders data
  #  clean_orders_data = data_cleaner.clean_orders_data(orders_data)

    # Upload cleaned data to orders_table
   # db_connector.upload_to_db(clean_orders_data, 'orders_table', local=True)

   # json_file_path = 'date_details.json'
   # json_data = data_extractor.extract_from_json(json_file_path)

    # Clean JSON data
   # clean_json_data = data_cleaner.clean_json_data(json_data)
    # Convert dict to DataFrame 
   # clean_json_data_df = pd.DataFrame.from_dict(clean_json_data)
    
    # Upload cleaned data to dim_date_times table
   # db_connector.upload_to_db(clean_json_data_df, 'dim_date_times', local=True)

if __name__ == "__main__":
    main()

