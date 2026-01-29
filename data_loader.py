import json
import csv

#Open the file in read mode : r
try:
    with open('config.json', 'r') as file:
        data = json.load(file)
        print("Success, Data loaded")
except FileNotFoundError:
    print("Error: File not foudn")


filePath = 'gdp.csv'

with open(filePath, mode = 'r', encoding = 'utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        country = row["Country Name"]
        code = row["Country Code"]
        gdp_1960 = row["1960"]

        print(f"{country} ({code}) had a GDP of {gdp_1960} in 1960.")

