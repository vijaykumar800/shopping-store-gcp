import json


def read_config():
    with open("config.json", "r") as file:
        return json.load(file)


CONFIG_JSON = read_config()