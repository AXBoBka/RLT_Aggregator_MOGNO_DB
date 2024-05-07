import os
import json


def parse_input_file(input_data_path: str):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, input_data_path)

    try:
        with open(full_path, "r") as file:
            data = json.load(file)
        return data
    except Exception as e:
        print(f'Невалидный json: {input_data_path} - {e}!')
        raise e
