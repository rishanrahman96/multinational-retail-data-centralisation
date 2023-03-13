from database_utils import DatabaseConnector
import sqlalchemy
import pandas as pd
from tabula.io import read_pdf
import requests
import boto3
from urllib.request import urlopen
import json


class DataExtractor:

    def __init__ (self):
        pass

    def read_dbs(self):
        connector = DatabaseConnector()
        engine = connector.read_db_creds()
        tables = connector.list_db_tables(engine)
        user_table = pd.read_sql_table(tables[1],con = engine.connect(), index_col='index')
        orders_table = pd.read_sql_table(tables[2],con = engine.connect(), index_col= 'index')
        return user_table, orders_table

    
    def retrieve_pdf_data(self):
        pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        dfs = read_pdf(pdf_path, stream =True, pages = 'all')
        return dfs
        
    def list_number_of_stores(self):
        Headers = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers = Headers)
        number_of_stores = response.json()['number_stores']
        return Headers, number_of_stores
    

    def retrieve_stores_data(self):
        stores = []
        Headers = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        for i in range(451):
            response = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}', headers = Headers)
            stores.append(response.json())
        return stores
    
    def retrieve_date_events(self):

        url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        df = pd.read_json(url)
        return df


# store the URL in url as 
# # parameter for urlopen
# url = "https://api.github.com"
  
# # store the response of URL
# response = urlopen(url)
  
# # storing the JSON response 
# # from url in data
# data_json = json.loads(response.read())

    
    

    #Commented out below to be efficient and not make the request lots of times.
    # def extract_from_s3(self):
    #     s3 = boto3.client('s3')
    #     s3.download_file('data-handling-public', 'products_csv', 'products_to_be_cleaned.csv')

    

  








# x = DataExtractor()
# x.read_dbs()
