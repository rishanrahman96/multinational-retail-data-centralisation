from data_extraction import DataExtractor
import pandas as pd

class DataCleaning:

    def __init__ (self):
        pass

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
        table['date_of_birth_new'] = pd.to_datetime(table['date_of_birth'],errors = 'coerce', format = '%Y-%m-%d')
        table['join_date'] = pd.to_datetime(table['join_date'],errors = 'coerce', format = '%Y-%m-%d')
        table['user_uuid'] = table['user_uuid'].astype('string')
        table_2 = table[table['join_date'].notna()]
        cleaned_null_strings = table_2[~table_2.apply(lambda row: row.astype(str).str.contains('NULL').any(), axis=1)]
        print(cleaned_null_strings.info())
        print(cleaned_null_strings)


        


x = DataCleaning()
x.clean_user_data()