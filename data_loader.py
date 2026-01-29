import json
import csv

#Open the file in read mode : r
def load_config(filename):
    try:
        with open('filename', 'r') as file:
           return json.load(file) 
    except FileNotFoundError:
        print("Error: Json file missing")

def load_data(filename):
    try:
        with open(filename, mode = 'r', encoding = 'utf-8') as file:
            reader = csv.DictReader(file)

            data_list = list(reader)
            return data_list
    except FileNotFoundError:
        print(f"Error: CSV file missing")
        return []
