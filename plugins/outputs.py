import json
from typing import Dict, Any

class ConsoleWriter:
    def write(self, results: Dict[str, Any]) -> None:
        # check if there is an error first
        if "error" in results:
            print(f"\n[!] Data Error: {results['error']}")
            return

        # get the metadata stuff
        metadata = results.get("metadata", {})
        operation = metadata.get("operation", "Unknown Operation")

        # figure out if it's one year or a range
        if "years_range" in metadata:
            time_label = f"{metadata['years_range'][0]} - {metadata['years_range'][-1]}"
        else:
            time_label = metadata.get("year", "Unknown Time")

        # print the top part
        print("\n" + "="*50)
        print(f" GDP ANALYSIS: {str(operation).replace('_', ' ').upper()} ({time_label})")
        print("="*50)

        # get data and print each row
        labels = results.get("labels", [])
        values = results.get("values", [])
        
        # see if we need a percent sign
        is_percentage = operation in ["gdp_growth_rate", "fastest_growing_continent", "continent_contribution"]

        for rank, (label, val) in enumerate(zip(labels, values), start=1):
            if is_percentage:
                print(f"{rank}. {label}: {val:,.2f}%")
            else:
                print(f"{rank}. {label}: ${val:,.2f}")

        # bottom line
        print("="*50 + "\n")

class FileWriter:
    def write(self, results: Dict[str, Any]) -> None:
        # stop if there is an error
        if "error" in results:
            print(f"\n[!] Data Error: {results['error']}")
            return
            
        # name of the save file
        output_file = "analysis_output.json"
        try:
            # try to save it as json
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=4)
            print(f"\n[+] Results successfully written to {output_file}")
        except Exception as e:
            print(f"\n[!] Error writing to file: {e}")
