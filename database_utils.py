import yaml
from typing import Annotated
from sqlalchemy import create_engine, text
import pandas as pd
import sqlalchemy.types as alchemy_types

class DatabaseConnector:

    def __init__(self, filepath: yaml, dialect = "postgresql", driver = "psycopg2"):
        if ".yaml" not in filepath:
            raise ValueError("This class can only be instantiated using yaml files.")
        self.dialect = dialect
        self.driver = driver
        self.engine = self.init_db_engine(filepath)
    
    def read_db_creds(self, filepath: yaml) -> dict:
        with open(filepath, "r") as stream:
            parsed_stream = yaml.safe_load(stream)
        return parsed_stream
    
    def credential_processor(self, to_process:dict) -> dict:
        processed = {}
        for key, value in to_process.items():
            if "host" in key.lower():
                processed["host"] = value
            elif "password" in key.lower():
                processed["password"] = value
            elif "user" in key.lower():
                processed["user"] = value
            elif "database" in key.lower():
                processed["database"] = value
            elif "port" in key.lower():
                processed["port"] = value
        return processed
    
    def db_url_constructor(self, credentials) -> Annotated[str, "database URL for Engine object creation"]:
        dialect = self.dialect
        driver = self.driver
        username = credentials["user"]
        password = credentials["password"]
        host = credentials["host"]
        port = credentials["port"]
        database = credentials["database"]
        db_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}"
        return db_url

    def init_db_engine(self, filepath: yaml) -> Annotated[object, "Engine object for interacting with database"]:
        connection_credentials = self.read_db_creds(filepath)
        processed_credentials = self.credential_processor(connection_credentials)
        engine = create_engine(self.db_url_constructor(processed_credentials))
        return engine
    
    def list_db_tables(self):
        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT table_name, table_schema FROM INFORMATION_SCHEMA.tables WHERE table_schema = 'public'"))
        for row in result:
            print(f"table_name: {row.table_name}, table_schema: {row.table_schema}")
        return "END."

    # Takes in a dataframe and maps the datatype of the columns to an appropriate PostgreSQL datatype
    def datatype_mapper(self, dataframe_to_upload: pd.DataFrame) -> dict:
        mapper = {}
        columns = dataframe_to_upload.columns
        for column in columns:
            if dataframe_to_upload[column].dtype == "O":
                if len(dataframe_to_upload[column].apply(lambda x: len(x)).unique()) == 1:
                    varchar_length = dataframe_to_upload[column].apply(lambda x: len(x)).unique()[0]
                    mapper[column] = alchemy_types.VARCHAR(varchar_length)
                else:
                    mapper[column] = alchemy_types.TEXT()
            elif isinstance(dataframe_to_upload[column].dtype, pd.CategoricalDtype):
                varchar_length = max(dataframe_to_upload[column].apply(lambda x: len(x)).unique())
                mapper[column] = alchemy_types.VARCHAR(varchar_length)
            elif dataframe_to_upload[column].dtype == "<M8[ns]":
                mapper[column] = alchemy_types.DATE()
        return mapper
    
    def upload_to_db (self, db_name: str, db_index_label: str, dataframe_to_upload: pd.DataFrame):
        column_datatype_mapper = self.datatype_mapper(dataframe_to_upload)
        dataframe_to_upload.to_sql(name = db_name, con = self.engine, index_label = db_index_label, dtype = column_datatype_mapper)