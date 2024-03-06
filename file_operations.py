import pandas as pd
import gcsfs
import json
from google.cloud import bigquery


def read_config():
    with open("ext_tabel_schema.json", "r") as file:
        return json.load(file)


column_data_types = read_config()


class ShopperFile:
    __client = bigquery.Client()
    __project_id = "shopping-store-415510"
    __db_name = "store_data"
    __dev_table_name = "development_shopper_table"
    __prd_table_name = "shopper_table"

    def __init__(self,excel_file_name,excel_bucket_name):
        self.excel_file_name = excel_file_name
        self.excel_bucket_name = excel_bucket_name
        self.dev_bucket_location = "gs://development-shopper-table/shopping_store_bq_external.csv"
        self.prd_bucket_location = "gs://shopper-table/shopping_store_bq_external.csv"
        self.fs = gcsfs.GCSFileSystem()
        self.dev_bucket_name = "development-shopper-table"
        self.prd_bucket_name = "shopper-table"

    def load_content(self):
        excel_file_uri = f"gs://{self.excel_bucket_name}/{self.excel_file_name}"
        with self.fs.open(excel_file_uri) as file:
            df = pd.read_excel(file, engine='openpyxl')
        return df

    def fix_column_data_types(self):
        df = self.load_content()
        for col, expected_dtype in column_data_types.items():
            if expected_dtype == 'str':
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) else pd.NA)
            elif expected_dtype == 'int':
                df[col] = df[col].apply(lambda x: x if isinstance(x, (int, float)) else pd.NA).astype('Int64',
                                                                                                     errors='ignore')
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
            query_result = query_job.result()

            for row in query_result:
                is_output_empty_or_error = False
                break
        except Exception as e:
            print(f"An error occurred during query execution: {e}")
        return is_output_empty_or_error

    def test_deploy(self):
        self.export_csv_to_gcs(gcs_bucket_location=self.dev_bucket_location, bucket_name=self.dev_bucket_name)
        is_output_empty_or_error = self.validate_external_table(table_name=self.__dev_table_name)
        return is_output_empty_or_error

    def production_deploy(self):
        is_output_empty_or_error = self.test_deploy()
        if is_output_empty_or_error:
            print("The output is empty or an error occurred.")
        else:
            self.export_csv_to_gcs(gcs_bucket_location=self.prd_bucket_location, bucket_name=self.prd_bucket_name)
            self.validate_external_table(table_name=self.__prd_table_name)























