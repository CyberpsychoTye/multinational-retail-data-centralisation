import pandas as pd
from typing import Annotated 
from database_utils import DatabaseConnector
from sqlalchemy import text
import tabula

class DataExtraction:

    def __init__(self, instance):
        if isinstance(instance, DatabaseConnector):
            self.engine = instance.engine
        else:
            raise ValueError("This class only accepts instances of the DatabaseConnector as parameters.")

    def list_db_tables(self):
        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT table_name, table_schema FROM INFORMATION_SCHEMA.tables WHERE table_schema = 'public'"))
        for row in result:
            print(f"table_name: {row.table_name}, table_schema: {row.table_schema}")
        return "END."
    
    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        dataframe = pd.read_sql(table_name, self.engine, index_col = "index")
        return dataframe
    
    def retrieve_pdf_data(self, filepath:Annotated[str, "URL or local filepath"], output_format:str = "dataframe", pages:int|str = "all", area:list = None) -> pd.DataFrame:
        if area == None:
            list_of_dfs = tabula.read_pdf(input_path = filepath, output_format = output_format, pages = pages)
            return list_of_dfs[0]
        else:
            list_of_dfs = tabula.read_pdf(input_path = filepath, output_format = output_format, pages = pages, area = area)
            return list_of_dfs[0]




