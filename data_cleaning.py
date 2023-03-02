from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import requests
import json


import pandas as pd
import numpy as np

class DataCleaning:

    def __init__ (self):
        self.connector = DatabaseConnector()
        self.engine = self.connector.upload_to_db()
        self.engine.connect()

    def clean_user_data(self):
        x = DataExtractor()

        table = x.read_dbs()
        table['first_name'] = table['first_name'].astype("string")
        table['last_name'] = table['last_name'].astype("string")
        table['company'] = table['company'].astype("string")
        table['email_address'] = table['email_address'].astype("string")
        table['country_code'] = table['country_code'].astype("category")
        table['address'] = table['address'].astype("string")
        table['country'] = table['country'].astype("string")
        table['country_code'] = table['country_code'].astype('category')
        table['phone_number'] = table['phone_number'].astype('string')
        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'],errors = 'coerce', format = '%Y-%m-%d')
        table['join_date'] = pd.to_datetime(table['join_date'],errors = 'coerce', format = '%Y-%m-%d')
        table['user_uuid'] = table['user_uuid'].astype('string')
        table_2 = table[table['join_date'].notna()]
        cleaned_null_strings = table_2[~table_2.apply(lambda row: row.astype(str).str.contains('NULL').any(), axis=1)]

        cleaned_null_strings.to_sql('legacy_users',self.engine, if_exists= 'replace')



    def clean_card_data(self):
            
        df = pd.read_csv("carddetails.csv")
        card_numbers = {"American Express":[15],'Diners Club / Carte Blanche':[14],"Discover":[16],'JCB 15 digit':[15],'JCB 16 digit':[16],'Maestro':range(12,20), 'Mastercard':[16],'VISA 13 digit':[13],'VISA 16 digit':[16],'VISA 19 digit':[19] }
        df['card_provider_check'] = df['card_provider'].isin(card_numbers.keys())
        df2 = df[~df['card_provider_check'] == False]
        df2.drop('card_provider_check',axis = 1,inplace=True)
        df2['date_payment_confirmed'] = pd.to_datetime(df2['date_payment_confirmed'])
        df2['date_payment_confirmed'].dt.strftime('%Y-%m-%d')
        df2['card_number'] = df2['card_number'].str.strip('?')
        df2['expiry_date'] = pd.to_datetime(df2['expiry_date'],format = '%m/%y')
        df2['card_provider'] = df2['card_provider'].astype('string')

        def card_valid(cn,cp):
            if len(cn) in card_numbers[cp]:
                return True

            return False

        df2['card_valid'] =df2.apply(lambda x: card_valid(x['card_number'],x['card_provider']),axis = 1)
        df3 = df2[~df2['card_valid'] == False]
        df3.drop('card_valid',axis = 1,inplace=True)

        df3.to_sql('dim_card_details',self.engine, if_exists= 'replace')


        #Cleaning store data 

        # stores = x.retrieve_stores_data()
        # with open("stores.json",'w') as f:
        #     json.dump(stores, f)
        # f.close

    def clean_store_data(self):


        stores_df = pd.read_json('stores.json')
        stores_df.drop('lat',axis = 1,inplace=True)
        continents = ['GB','DE','US']
        stores_df['code_check'] = stores_df['country_code'].isin(continents)
        stores_df1 = stores_df[~stores_df['code_check'] == False]
        stores_df1['continent'] = stores_df1['continent'].replace(['eeEurope'],'Europe')
        stores_df1['continent'] = stores_df1['continent'].replace(['eeAmerica'],'America')
        stores_df1 = stores_df1.replace({'address': {'N/A':np.nan}, 'longitude': {'N/A':np.nan},'locality': {'N/A':np.nan}, 'latitude': {None: np.nan}})
        stores_df1 = stores_df1.replace({'staff_numbers' : {'J78': np.nan, '30e':np.nan,'80R':np.nan,'A97':np.nan,'3n9':np.nan}})
        stores_df1.drop('code_check',axis = 1,inplace=True)
        stores_df1['opening_date'] = pd.to_datetime(stores_df1['opening_date'])
        stores_df1['opening_date'].dt.strftime('%Y-%m-%d')
        stores_df1['address'] = stores_df1['address'].astype('string')
        stores_df1['continent'] = stores_df1['continent'].astype('category')
        stores_df1['longitude'] = stores_df1['longitude'].astype('float')
        stores_df1['locality'] = stores_df1['locality'].astype('string')
        stores_df1['store_code'] = stores_df1['store_code'].astype('category')
        stores_df1['staff_numbers'] = stores_df1['staff_numbers'].astype('float')
        stores_df1['latitude'] = stores_df1['latitude'].astype('float')
        stores_df1['country_code'] = stores_df1['country_code'].astype('category')
        stores_df1['store_type'] = stores_df1['store_type'].astype('category')

        stores_df1.to_sql('dim_store_details',self.engine,if_exists='replace')

    def clean_products_data(self):

        products_df = pd.read_csv('mrdc_products.csv')
        pd.set_option('display.max_rows', 2000)
        products_df.drop('Unnamed: 0', axis = 1, inplace=True)
        products_df = products_df[~products_df.isna().any(axis=1)]
        products_df.drop(index= [751,1400,1133], inplace=True)
        products_df.reset_index()
        products_df['product_name'] = products_df['product_name'].astype('string')
        products_df['EAN'] = products_df['EAN'].astype('string')
        products_df['uuid'] = products_df['uuid'].astype('string')
        products_df['category'] = products_df['category'].astype('category')
        products_df['date_added'] = pd.to_datetime(products_df['date_added'])
        products_df['date_added'].dt.strftime('%Y-%m-%d')
        products_df['removed'] = products_df['removed'].astype('category')
        products_df['product_code'] = products_df['product_code'].astype('category')

        def clean_weights(x):
            if 'x' in x and 'g' in x:
                x = x.replace('x','')
                x = x.replace('g','')
                x = x.split()
                x = [float(i) for i in x]
                x = np.prod(x)/1000
                return x
                
            elif 'kg' in x:
                x = float(x.strip('kg'))
                return x
            elif 'ml' in x or 'g' in x:
                x = float(x.strip('mlg .'))/1000
                
                return x
            elif 'oz' in x:
                x = float(x.strip('oz'))/35.27
                

        products_df['weight (kg)'] =products_df.apply(lambda x: clean_weights(x['weight']),axis = 1)

        products_df.to_sql('dim_products',self.engine,if_exists='replace')



    
        # connector = DatabaseConnector()
        # engine = connector.upload_to_db()
        # engine.connect()




        # df3.to_sql('dim_card_details',engine, if_exists= 'replace')
        # stores_df1.to_sql('dim_store_details',engine,if_exists='replace')
        # cleaned_null_strings.to_sql('legacy_users',engine, if_exists='replace')







x = DataCleaning()
x.clean_user_data()
x.clean_products_data()
x.clean_card_data()
