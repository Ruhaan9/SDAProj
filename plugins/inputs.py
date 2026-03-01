import json
import re
from typing import Any

class JsonReader:
    def __init__(self, service_handler: Any, file_path: str):
        # save the handler and file path
        self.service_handler = service_handler
        self.file_path = file_path

    def run(self) -> None:
        print(f"Reading JSON data from {self.file_path}...")

        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                content = file.read()

            # Replace NaN with null to prevent JSONDecodeError
            content = re.sub(r'\bNaN\b', 'null', content)

            # Split into individual JSON objects safely
            objects = []
            decoder = json.JSONDecoder()
            idx = 0
            length = len(content)

            while idx < length:
                # Remove leading whitespace to avoid decoder errors
                remaining_content = content[idx:].lstrip()
                if not remaining_content:
                    break
                
                try:
                    obj, offset = decoder.raw_decode(remaining_content)
                    objects.append(obj)
                    # Update index based on whitespace removed + characters decoded
                    idx += (len(content[idx:]) - len(remaining_content)) + offset
                except json.JSONDecodeError:
                    idx += 1  # move forward and try again

            if not objects:
                print("No valid JSON objects found.")
                return

            # Keep only valid dictionary records for the GDPEngine
            valid_rows = [obj for obj in objects if isinstance(obj, dict)]

            if not valid_rows:
                print("No valid JSON objects found.")
                return

            # Send the cleaned data to the main service (GDPEngine)
            self.service_handler.execute(valid_rows)

        except FileNotFoundError:
            print(f"Error: The file {self.file_path} was not found.")