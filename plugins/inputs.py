import json
from typing import Any

class JsonReader:
    def __init__(self, service_handler: Any, file_path: str):
        # save the handler and file path
        self.service_handler = service_handler
        self.file_path = file_path

    def run(self) -> None:
        print(f"Reading JSON data from {self.file_path}...")
        try:
            # open the file and load the json
            with open(self.file_path, 'r', encoding='utf-8') as file:
                raw_data = json.load(file)

            # send the data to the main service
            self.service_handler.execute(raw_data)

        except FileNotFoundError:
            print(f"Error: The file {self.file_path} was not found.")
        except json.JSONDecodeError:
            print(f"Error: {self.file_path} is not a valid JSON file.")
