import yaml
from typing import Annotated
from sqlalchemy import create_engine, text

class DatabaseConnector:
    
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
    
    def db_url_constructor(self, credentials, dialect = "postgresql", driver = "psycopg2") -> Annotated[str, "database URL for Engine object creation"]:
        dialect = dialect
        driver = driver
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
    
    def list_db_tables(self, engine: object):
        with engine.connect() as connection:
            result = connection.execute(text("SELECT table_name, table_schema FROM INFORMATION_SCHEMA.tables WHERE table_schema = 'public'"))
        for row in result:
            print(f"table_name: {row.table_name}, table_schema: {row.table_schema}")
        return "END."




hello = DatabaseConnector()


engine = hello.init_db_engine("db_creds.yaml")

print(hello.list_db_tables(engine))

# def credential_processor(to_process:dict) -> dict:
#     processed = {}
#     for key, value in to_process.items():
#         if "host" in key.lower():
#             processed["host"] = value
#         elif "password" in key.lower():
#             processed["password"] = value
#         elif "user" in key.lower():
#             processed["user"] = value
#         elif "database" in key.lower():
#             processed["database"] = value
#         elif "port" in key.lower():
#             processed["port"] = value
#     return processed

# def db_url_constructor(credentials, dialect = "postgresql", driver = "psycopg2") -> Annotated[str, "database URL for Engine object creation"]:
#     dialect = dialect
#     driver = driver
#     username = credentials["user"]
#     password = credentials["password"]
#     host = credentials["host"]
#     port = credentials["port"]
#     database = credentials["database"]
#     db_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}"

#     return db_url


