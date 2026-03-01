import json
import sys

# import our stuff
from plugins import JsonReader
from plugins import ConsoleWriter
from core import GDPEngine

# set up dictionaries for the plugins
INPUT_DRIVERS = {
    "json": JsonReader
}

OUTPUT_DRIVERS = {
    "console": ConsoleWriter
}

def bootstrap():
    print("Bootstrapping GDP Analysis System...")

    # try to open and read the config file
    try:
        with open("config.json", "r") as file:
            config = json.load(file)
    except FileNotFoundError:
        print("Error: config.json not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Error: config.json is not valid JSON.")
        sys.exit(1)

    # get io settings
    io_settings = config.get("io_settings", {})

    # grab settings from the config
    in_source = io_settings.get("input_source", "json")
    in_path = io_settings.get("input_path", "gdp_with_continent_filled.json")
    out_sink = io_settings.get("output_sink", "console")
    analysis_settings = config.get("analysis_settings", {})

    # setup the output part
    OutputClass = OUTPUT_DRIVERS.get(out_sink)
    if not OutputClass:
        print(f"Error: Unknown output sink '{out_sink}'")
        sys.exit(1)
    sink = OutputClass()

    # setup the main engine and pass it the output sink
    engine = GDPEngine(sink=sink, config=analysis_settings)

    # setup the input part and pass it the engine
    InputClass = INPUT_DRIVERS.get(in_source)
    if not InputClass:
        print(f"Error: Unknown input source '{in_source}'")
        sys.exit(1)
    reader = InputClass(service_handler=engine, file_path=in_path)

    # start it up
    reader.run()

if __name__ == "__main__":
    bootstrap()
