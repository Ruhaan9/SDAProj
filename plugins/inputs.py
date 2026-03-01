import json
import csv
from typing import Any

class JsonReader:
    def __init__(self, service_handler: Any, file_path: str):
        # save the handler and file path
        self.service_handler = service_handler
        self.file_path = file_path

    def run(self) -> None:
        print(f"Reading JSON data from {self.file_path}...")
        try:
            # open and load json file
            with open(self.file_path, 'r', encoding='utf-8') as file:
                # handles nan natively
                objects = json.load(file) 

            # keep only valid dictionaries
            valid_rows = [obj for obj in objects if isinstance(obj, dict)]

            if not valid_rows:
                print("No valid JSON objects found.")
                return

            # send data to the engine
            self.service_handler.execute(valid_rows)

        except FileNotFoundError:
            print(f"Error: The file {self.file_path} was not found.")
        except Exception as e:
            print(f"Error loading JSON: {e}")

class CsvReader:
    def __init__(self, service_handler: Any, file_path: str):
        # save the handler and file path
        self.service_handler = service_handler
        self.file_path = file_path

    def run(self) -> None:
        print(f"Reading CSV data from {self.file_path}...")
        try:
            # open the csv file
            with open(self.file_path, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                # make csv into a list of dicts
                data = [row for row in reader]
            
            if not data:
                print("No valid CSV rows found.")
                return
                
            # send data to the engine
            self.service_handler.execute(data)
            
        except FileNotFoundError:
            print(f"Error: The file {self.file_path} was not found.")
        except Exception as e:
             print(f"Error loading CSV: {e}")
