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


def load_postal_code_df():
    print("entering load_postal_code_df")
    postal_code_file_uri = CONFIG_JSON["GCS_BUCKET_PATH"]["POSTAL_BUCKET_LOCATION"]
    df = pd.read_csv(postal_code_file_uri)
    df_post = pd.DataFrame().assign(pincode=df['pincode'], city=df['regionname'])
    postal_df = df_post.drop_duplicates(subset=['pincode'], keep='first')
    print("end load_postal_code_df")
    print(postal_df.head(5))
    return postal_df


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
        print("entering load_content")
        excel_file_uri = f"gs://{self.excel_bucket_name}/{self.excel_file_name}"
        print(excel_file_uri)
        print("entering second line")
        with self.fs.open(excel_file_uri) as file:
            print("entering third line")
            df = pd.read_excel(file, engine='openpyxl')
            print("entering fourth line")
            print(df)
        print("entering fifth line")
        print(df)
        print("end load_content")
        return df

    @staticmethod
    def clean_sex_column(value):
        print("entering clean_sex_column")
        if pd.isna(value):
            return value

        male_pattern = re.compile(r'male', re.IGNORECASE)
        female_pattern = re.compile(r'female', re.IGNORECASE)
        if female_pattern.search(value):
            return "female"
        elif value != "female":
            male_pattern.search(value)
            return "male"
        else:
            return pd.NA
        print("end clean_sex_column")

    def fix_column_data_types(self):
        print("entering fix_column_data_types")
        df = self.load_content()
        for col, expected_dtype in column_data_types.items():
            if expected_dtype == 'str':
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) else pd.NA)
            elif expected_dtype == 'int':
                df[col] = df[col].apply(lambda x: x if isinstance(x, (int, float)) else pd.NA).astype('Int64',
                                                                                                     errors='ignore')
        df['sex'] = df['sex'].apply(self.clean_sex_column)
        print(df)
        print("end fix_column_data_types")
        return df

    def city_column(self):
        print("entering city_column")
        df_main = self.fix_column_data_types()
        df_clean = pd.merge(df_main, load_postal_code_df(), how='left', left_on='pincode', right_on='pincode')
        print(df_clean.head(5))
        print("end city_column")
        return df_clean

    def export_csv_to_gcs(self, gcs_bucket_location=None, bucket_name=None):
        print("entering export_csv_to_gcs")
        df = self.city_column()
        bucket_files = list(self.fs.ls(bucket_name))
        if bucket_files:
            for filepath in bucket_files:
                self.fs.rm(filepath, recursive=True)
        with self.fs.open(gcs_bucket_location, 'w') as f:
            df.to_csv(f, index=False)
        print("end export_csv_to_gcs")

    def validate_external_table(self,table_name=None):
        print("entering validate_external_table")
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
        print("entering test_deploy")
        self.export_csv_to_gcs(gcs_bucket_location=self.dev_bucket_location, bucket_name=self.dev_bucket_name)
        is_output_empty_or_error = self.validate_external_table(table_name=self.__dev_table_name)
        print("end test_deploy")
        return is_output_empty_or_error

    def production_deploy(self):
        print("entering production_deploy")
        is_dev_output_empty_or_error = self.test_deploy()
        if is_dev_output_empty_or_error:
            print("The output is empty or an error occurred in development phase.")
        else:
            self.export_csv_to_gcs(gcs_bucket_location=self.prd_bucket_location, bucket_name=self.prd_bucket_name)
            is_prd_output_empty_or_error = self.validate_external_table(table_name=self.__prd_table_name)
            if is_prd_output_empty_or_error:
                print("The output is empty or an error occurred in production stage.")
        print("end production_deploy")

























