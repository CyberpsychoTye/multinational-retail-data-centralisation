from database_utils import DatabaseConnector
from data_extraction import DataExtraction
from data_cleaning import DataCleaning

#Instantiating a cleaner instance to clean all extracted data. 
cleaner = DataCleaning()

#The code block below extracts, cleans, and uploads the legacy_users data to the local_sales_data database
connector = DatabaseConnector("db_creds.yaml")
extractor = DataExtraction(connector)
user_data_df = extractor.read_rds_table("legacy_users")
processed_user_data = cleaner.clean_user_data(user_data_df)
local_sales_data_connector = DatabaseConnector("local_sales_data_creds.yaml")
local_sales_data_connector.upload_to_db("dim_users", "index", processed_user_data)
