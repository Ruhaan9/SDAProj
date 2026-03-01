from typing import Dict, Any

class ConsoleWriter:
    def write(self, results: Dict[str, Any]) -> None:
        # check if there is an error first
        if "error" in results:
            print(f"\n[!] Data Error: {results['error']}")
            return

        # get the year and operation stuff
        metadata = results.get("metadata", {})
        year = metadata.get("year", "Unknown Year")
        operation = metadata.get("operation", "Unknown Operation")

        # print the top part
        print("\n" + "="*50)
        print(f" GDP ANALYSIS: {str(operation).replace('_', ' ').upper()} ({year})")
        print("="*50)

        # get data and print each row
        labels = results.get("labels", [])
        values = results.get("values", [])

        for rank, (country, gdp) in enumerate(zip(labels, values), start=1):
            print(f"{rank}. {country}: ${gdp:,.2f}")

        # bottom line
        print("="*50 + "\n")
