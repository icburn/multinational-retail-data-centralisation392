# data_cleaning.py
import pandas as pd
import re
from datetime import datetime
from dateutil import parser

class DataCleaning:
    def __init__(self):
        pass

    def clean_csv_data(self, data):
        # Example: data.dropna(inplace=True) to remove rows with NaN values
        return data

    def clean_api_data(self, data):
        # Example: Convert JSON to DataFrame and perform cleaning
        return data

    def clean_s3_data(self, data):
        # Example: Parse and clean the string data retrieved from S3
        return data

    def clean_rds_data(self, data):
        # Example: data = data[data['column_name'].notna()] to remove rows with NaN in a specific column
        return data

    def clean_user_data(self, df):
        
        # Replace 'GGB' with 'GB' in 'country_code'
        df['country_code'] = df['country_code'].replace('GGB', 'GB')

        # Filter for rows with 'country_code' as 'DE', 'GB', 'US'
        df = df[df['country_code'].isin(['DE', 'GB', 'US'])]

        return df

    def clean_card_data(self, df):

        # Remove all non-numerical characters from 'card_number'
        df['card_number'] = df['card_number'].astype(str).str.replace('[^0-9]', '', regex=True)


        # Convert 'expiry_date' to datetime, invalid dates become NaT
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df.dropna(subset=['expiry_date'], inplace=True)

        def flexible_date_parser(date_str):
            try:
                return parser.parse(date_str)
            except (ValueError, TypeError):
                return pd.NaT

        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(flexible_date_parser)
        df.dropna(subset=['date_payment_confirmed'], inplace=True)

        # Further cleaning operations...
        return df
    
    def clean_store_data(self, df):
        # Create a copy of the DataFrame
        df_cleaned = df.copy()

        # Remove rows where 'country_code' is not one of 'GB', 'US', 'DE'
        df_cleaned = df_cleaned[df_cleaned['country_code'].isin(['GB', 'US', 'DE'])]

        # Check if 'opening_date' column exists before filtering
        if 'opening_date' in df_cleaned.columns:
            df_cleaned = df_cleaned[df_cleaned['opening_date'].notnull()]
        else:
            print("The 'opening_date' column is not found in the DataFrame.")

        # Standardize 'opening_date'
        def standardize_date(date):
            if pd.isna(date):
                return pd.NaT  # Return Not-a-Time for NaN dates

            try:
                # Attempt to convert to datetime directly (works for 'yyyy-mm-dd')
                return pd.to_datetime(date)
            except:
                # For other formats, try to parse manually
                try:
                    # For 'yyyy Month dd' format
                    return pd.to_datetime(date, format='%Y %B %d')
                except:
                    try:
                        # For 'Month yyyy dd' format
                        return pd.to_datetime(date, format='%B %Y %d')
                    except:
                        # If all conversions fail, return Not-a-Time
                        return pd.NaT

        df_cleaned['opening_date'] = df_cleaned['opening_date'].apply(standardize_date)

        return df_cleaned
    
    def clean_products_data(self, df):
        # Clean product price
        df['product_price'] = df['product_price'].str.replace('£', '')
      #  df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')  # Convert to float, non-numeric become NaN

        def convert_weight(weight):
            # Check if the weight is not a number (NaN) or None
       #     if pd.isna(weight) or weight is None:
        #        return None  # Or a default value like 0, depending on your requirements

            # Convert to string for processing
            weight_str = str(weight).strip()

            # Handle compound weights like '12 x 100'
            if ' x ' in weight_str:
                try:
                    parts = weight_str.split(' x ')
                    product = 1
                    for part in parts:
                        product *= float(part)
                    return product
                except ValueError:
                    # If conversion fails, pass to the next checks
                    pass

            # Process the weight based on its unit
            try:
                if 'kg' in weight_str:
                    return float(weight_str.replace('kg', '').strip())
                elif 'g' in weight_str:
                    return float(weight_str.replace('g', '').strip()) / 1000
                elif 'ml' in weight_str:
                    return float(weight_str.replace('ml', '').strip()) / 1000
                else:
                    # Directly convert to float
                    return float(weight_str)
            except ValueError:
                return None  # Return None or some default value for unhandled cases



        df['weight'] = df['weight'].apply(convert_weight)

       # Convert 'date_added' to a datetime object
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

       # Standardize 'removed' column to boolean (True/False)
        df['removed'] = df['removed'] == 'Still_avaliable'

         # Remove rows where 'uuid' length is less than 15 characters
        df = df[df['uuid'].str.len() >= 15]

          # Remove rows where 'weight' is null
       # df = df.dropna(subset=['weight'])

        return df



    def clean_orders_data(self, df):
        # Remove specified columns
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        # Further cleaning logic if required
        return df

    def clean_json_data(self, df):
        # Example: Normalize data, handle missing values, etc.
        return df