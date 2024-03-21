import file_operations
import json


def read_config():
    with open("ext_tabel_schema.json", "r") as file:
        return json.load(file)


tabel_schema_json = read_config()


def hello_gcs(event, context):
    print("Processing Google Cloud Storage event...")
    excel_bucket_name = event['bucket']
    excel_file_name = event['name']
    print(f"Received Excel file: {excel_file_name} from bucket: {excel_bucket_name}")
    store = file_operations.ShopperFile(excel_file_name, excel_bucket_name)
    store.production_deploy()
