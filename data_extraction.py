from database_utils import DatabaseConnector
import sqlalchemy
import pandas as pd


class DataExtractor:

    def __init__ (self):
        pass

    def read_dbs(self):
        connector = DatabaseConnector()
        engine = connector.read_db_creds()
        tables = connector.list_db_tables(engine)
        table = pd.read_sql_table(tables[1],con = engine.connect())
        return table


# x = DataExtractor()
# x.read_dbs()
