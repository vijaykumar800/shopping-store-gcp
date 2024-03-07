import pandas as pd
import gcsfs
import json
from google.cloud import bigquery
from config import CONFIG_JSON
import re


def read_config():
    with open("ext_tabel_schema.json", "r") as file:
        return json.load(file)


column_data_types = read_config()


class ShopperFile:
    __client = bigquery.Client()
    __project_id = CONFIG_JSON["GOOGLE_CONFIG"]["PROJECT_ID"]
    __db_name = CONFIG_JSON["GOOGLE_CONFIG"]["DB_NAME"]
    __dev_table_name = CONFIG_JSON["GOOGLE_CONFIG"]["DEV_TABLE_NAME"]
    __prd_table_name = CONFIG_JSON["GOOGLE_CONFIG"]["PRD_TABLE_NAME"]

    def __init__(self,excel_file_name,excel_bucket_name):
        self.excel_file_name = excel_file_name
        self.excel_bucket_name = excel_bucket_name
        self.dev_bucket_location = CONFIG_JSON["GCS_BUCKET_PATH"]["DEV_BUCKET_LOCATION"]
        self.prd_bucket_location = CONFIG_JSON["GCS_BUCKET_PATH"]["PRD_BUCKET_LOCATION"]
        self.fs = gcsfs.GCSFileSystem()
        self.dev_bucket_name = CONFIG_JSON["GCS_BUCKET_PATH"]["DEV_BUCKET_NAME"]
        self.prd_bucket_name = CONFIG_JSON["GCS_BUCKET_PATH"]["PRD_BUCKET_NAME"]

    def load_content(self):
        excel_file_uri = f"gs://{self.excel_bucket_name}/{self.excel_file_name}"
        with self.fs.open(excel_file_uri) as file:
            df = pd.read_excel(file, engine='openpyxl')
        return df

    @staticmethod
    def clean_sex_column(value):
        if value == 'NA':
            return value
        else:
            male_pattern = re.compile(r'male', re.IGNORECASE)
            female_pattern = re.compile(r'female', re.IGNORECASE)
            if male_pattern.search(value):
                return "male"
            elif female_pattern.search(value):
                return "female"
            else:
                return 'NA'

    def fix_column_data_types(self):
        df = self.load_content()
        for col, expected_dtype in column_data_types.items():
            if expected_dtype == 'str':
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) else pd.NA)
            elif expected_dtype == 'int':
                df[col] = df[col].apply(lambda x: x if isinstance(x, (int, float)) else pd.NA).astype('Int64',
                                                                                                     errors='ignore')
        df['sex'] = df['sex'].apply(self.clean_sex_column)
        print(df)
        return df

    def export_csv_to_gcs(self, gcs_bucket_location=None, bucket_name=None):
        df = self.fix_column_data_types()
        bucket_files = list(self.fs.ls(bucket_name))
        if bucket_files:
            for filepath in bucket_files:
                self.fs.rm(filepath, recursive=True)
        with self.fs.open(gcs_bucket_location, 'w') as f:
            df.to_csv(f, index=False)

    def validate_external_table(self,table_name=None):
        select_query = f"""select * from {self.__project_id}.{self.__db_name}.{table_name};"""
        is_output_empty_or_error = True
        try:
            query_job = self.__client.query(select_query)
            query_job_result = query_job.result()
            query_job_row_count = query_job_result.total_rows
            print("number of rows in the table", query_job_row_count)

            if query_job_row_count > 0:
                is_output_empty_or_error = False

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return is_output_empty_or_error

    def test_deploy(self):
        self.export_csv_to_gcs(gcs_bucket_location=self.dev_bucket_location, bucket_name=self.dev_bucket_name)
        is_output_empty_or_error = self.validate_external_table(table_name=self.__dev_table_name)
        return is_output_empty_or_error

    def production_deploy(self):
        is_dev_output_empty_or_error = self.test_deploy()
        if is_dev_output_empty_or_error:
            print("The output is empty or an error occurred in development phase.")
        else:
            self.export_csv_to_gcs(gcs_bucket_location=self.prd_bucket_location, bucket_name=self.prd_bucket_name)
            is_prd_output_empty_or_error = self.validate_external_table(table_name=self.__prd_table_name)
            if is_prd_output_empty_or_error:
                print("The output is empty or an error occurred in production stage.")

























