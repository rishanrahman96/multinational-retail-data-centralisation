import yaml
from yaml.loader import SafeLoader
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy import inspect


class DatabaseConnector:

    def __init__ (self):
        pass

    def list_db_tables(self,engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(tables)
        return tables
    
    def init_db_engine(self,data):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['RDS_HOST']
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE = data['RDS_DATABASE']
        PORT =  data['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        self.list_db_tables(engine)
        return engine
        # engine.connect() - mainly used when you are getting data from database, see your list_db_tables
        
    
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            data = yaml.load(f, Loader= SafeLoader)
        return self.init_db_engine(data)
    

    def upload_to_db(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Verygood123!'
        DATABASE = 'Sales_Data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
        


# connector = DatabaseConnector()
# connector.upload_to_db()