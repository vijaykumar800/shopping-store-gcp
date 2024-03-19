import file_operations
import json


def read_config():
    with open("ext_tabel_schema.json", "r") as file:
        return json.load(file)


tabel_schema_json = read_config()


def hello_gcs(event, context):
    print("entering gcs function")
    excel_bucket_name = event['bucket']
    excel_file_name = event['name']
    store = file_operations.ShopperFile(excel_file_name, excel_bucket_name)
    store.production_deploy()
