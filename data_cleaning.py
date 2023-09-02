import re
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtraction


class DataCleaning:

    def __init__(self, extractor_instance: object):
        if isinstance(extractor_instance, DataExtraction):
            self.extractor = extractor_instance

    def date_converter(self, x):
        if x[0].lower() == "january":
            return f"{x[1]}-01-{x[2]}"

        elif x[0].lower() == "november":
            return f"{x[1]}-11-{x[2]}"

        elif x[0].lower() == "june":
            return f"{x[1]}-06-{x[2]}"

        elif x[0].lower() == "february":
            return f"{x[1]}-02-{x[2]}"

        elif x[0].lower() == "july":
            return f"{x[1]}-07-{x[2]}"

        elif x[0].lower() == "december":
            return f"{x[1]}-12-{x[2]}"

        elif x[0].lower() == "may":
            return f"{x[1]}-05-{x[2]}"
        
    """
    This section comprises a suite of methods used to clean phone number data. 
    Employs Regex to catch the different phone number formats and cleans them (Regex fun ;) ).
    Utilised in the cleaning phone number stage in the clean_user_data pipeline method.
    Each method includes comments to outline its objective.
    """
    # Removes extra digits (i.e 'x000') from numbers
    def extra_numbers_remover(self, x):
        if re.search(r"x\d+", x):
            return re.sub(r"x\d+", "", x)
        return x
    
    # Removes "+1" and "001" (US exit codes) from beginning of numbers
    def us_exit_code_remover(self, x):
        if re.search(r"^001", x) or re.search(r"^\+1", x):
            if re.search(r"^001", x):
                return re.sub(r"^001", "", x)
            else:
                return re.sub(r"^\+1", "", x)
        return x
    
    # Removes brackets () from numbers
    def bracket_remover(self, x):
        if re.search(r"^\(\d\d+\)", x):
            match = re.search(r"^\(\d\d+\)", x).group()
            return re.sub(r"^\(\d\d+\)", match[1:len(match)-1], x)
        return x
    
    # Removes "+44(0)/+49(0)" (UK and Germany codes) from numbers
    def europe_code_remover(self, x):
        if re.search(r"^\+4\d\(0\)", x):
            return re.sub(r"^\+4\d\(0\)", "0", x)
        return x
    
    # Removes +44 (UK code) from numbers 
    def uk_code_remover (self, x):
        if re.search(r"\+44", x):
            return re.sub(r"\+44", "0", x)
        return x
    
    # Pipeline method for cleaning the "legacy_users" table
    def clean_user_data(self, tablename: str) -> pd.DataFrame:
        df = self.extractor.read_rds_table(tablename)
        df = df.sort_values(by = ["index"]) 
        # Drops rows with invalid data inputs:
        valid_country_codes_bool= df["country_code"].apply(lambda x: len(x) > 4)
        rows_to_remove = df[valid_country_codes_bool].index
        df = df.drop(rows_to_remove)
        # Replaces the "NULL" values in the table with Pandas recognised missing values and removes rows with missing values:
        df.where((df != "NULL"), inplace = True)
        df.dropna(how = "all", inplace = True)
        # Converts inputs like ("19 October 1980") in "date_of_birth" column into ISO format and changes column data type to datetime:
        to_replace_bool = df["date_of_birth"].apply(lambda x: x[0:2] not in ["19", "20"])
        to_replace = df["date_of_birth"][to_replace_bool]
        to_replace = to_replace.apply(lambda x: x.split())
        to_replace = to_replace.apply(self.date_converter)
        df.update(to_replace)
        df["date_of_birth"]= pd.to_datetime(df["date_of_birth"], format = "mixed")
        # Converts addresses in the "address" column into more readble format:
        df["address"] = df["address"].apply(lambda x: x.replace("\n", ", "))
        # Changes invalid input (like "GGB") in country_codes to correct inputs:
        df["country_code"].where(df["country_code"] != "GGB", "GB", inplace = True)

        # Cleans the "phone_number" column:
        df["phone_number"] = df["phone_number"].apply(lambda x: x.replace(" ", ""))
        df["phone_number"] = df["phone_number"].apply(self.extra_numbers_remover)
        # Removes invalid characters like "-" and "." from numbers:
        df["phone_number"] = df["phone_number"].apply(lambda x: x.replace("-", ""))
        df["phone_number"] = df["phone_number"].apply(lambda x: x.replace(".", ""))
        df["phone_number"] = df["phone_number"].apply(self.us_exit_code_remover)
        df["phone_number"] = df["phone_number"].apply(self.bracket_remover)
        df["phone_number"] = df["phone_number"].apply(self.europe_code_remover)
        df["phone_number"] = df["phone_number"].apply(self.uk_code_remover)
        # Converts input in the "join_date" column to ISO format:
        df["join_date"] = df["join_date"].apply(lambda x: pd.to_datetime(x, format = "mixed"))

        return df
    
connector = DatabaseConnector("db_creds.yaml")
extractor = DataExtraction(connector)
cleaner = DataCleaning(extractor)

hello = cleaner.clean_user_data("legacy_users")