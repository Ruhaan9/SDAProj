# plugins/inputs.py
import json
import csv
from core.contracts import PipelineService

class JsonReader:
    #explicit definition for PipelineService
    def __init__(self, service_handler: PipelineService, file_path: str):
        # save the handler and file path
        self.service_handler = service_handler
        self.file_path = file_path

    def run(self) -> None:
        print(f"Reading JSON data from {self.file_path}...")
        try:
            # fix bad tokens line by line like nan or missing stuff
            lines = []
            with open(self.file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    # look for key value pairs
                    if '":' in line:
                        # split on the first colon so we dont mess up strings
                        parts = line.split('":', 1)
                        key = parts[0] + '"'
                        val_original = parts[1].rstrip('\n\r')
                        val_stripped = val_original.strip()
                        
                        has_comma = val_stripped.endswith(',')
                        if has_comma:
                            val = val_stripped[:-1].strip()
                        else:
                            val = val_stripped

                        # check if the value is ok
                        is_valid = False
                        if val:
                            # valid json values start with these characters
                            if val[0] in '"[{tfn-0123456789':
                                is_valid = True
                            # block weird python or pandas tokens
                            if val.lower() in ['nan', 'none', 'na', 'n/a', 'inf', '-inf', '']:
                                is_valid = False
                        
                        # if its bad or empty just make it null
                        if not is_valid:
                            val = "null"

                        # put the line back together
                        new_line = f"{key}: {val}{',' if has_comma else ''}\n"
                        lines.append(new_line)
                    else:
                        lines.append(line)

            cleaned_data = "".join(lines)
            
            objects = None
            try:
                # load the cleaned string
                objects = json.loads(cleaned_data) 
            except json.JSONDecodeError as e:
                # try to save the file if its cut off or broken
                print(f"Warning: JSON file is truncated or corrupted (Error: {e}). Attempting to salvage...")
                
                # find where the last dictionary ends
                last_brace = cleaned_data.rfind('}')
                if last_brace != -1:
                    # cut off the broken part and close the array
                    salvaged_data = cleaned_data[:last_brace + 1] + '\n]'
                    try:
                        objects = json.loads(salvaged_data)
                        print(f"Successfully salvaged {len(objects)} valid rows.")
                    except json.JSONDecodeError as e2:
                        raise ValueError(f"Salvage failed. Data is too corrupted: {e2}")
                else:
                    raise ValueError("Could not find any complete JSON objects to salvage.")

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
    #explicit definition for PipelineService
    def __init__(self, service_handler: PipelineService, file_path: str):
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