import matplotlib.pyplot as plt
from data_loader import load_data, load_config
from processor import process_data

def dashboard():
    print("GDP Analytics Dashboard")

    config = load_config("config.json")
    data = load_data("gdp.csv")

    if not config or not data:
        print("Error: Could not load a file")
        return

    results = process_data(data, config)
    if "error" in results or "Error" in results:
        print("Data Error:")
        print(results)
        return
# ^Data handling and validation

