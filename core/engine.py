from typing import List, Dict, Any
from core.contracts import DataSink

class GDPEngine:
    def __init__(self, sink: DataSink, config: Dict[str, Any]):
        self.sink = sink
        self.config = config

    def execute(self, raw_data: List[Dict[str, Any]]) -> None:
        year = self.config.get("year")
        operation = self.config.get("operation")
        r_type = self.config.get("region_type")
        r_val = self.config.get("region_value")

        filtered = [
            row for row in raw_data 
            if row.get(r_type) == r_val and self._is_valid(row.get(year))
        ]

        if not filtered:
            self.sink.write({"error": "No data matches criteria", "data": []})
            return

        processed_results = self._transform(filtered, year, operation)
        self.sink.write(processed_results)

    def _is_valid(self, value: Any) -> bool:
        try:
            return value is not None and str(value).strip() != ""
        except:
            return False

    def _transform(self, data: List[Dict], year: str, op: str) -> Dict[str, Any]:
        # Implementation of business logic (e.g., sorting for Top 10)
        sorted_list = sorted(
            data, 
            key=lambda x: float(x[year]), 
            reverse=True
        )
        
        top_n = sorted_list[:10] if op == "top_10" else sorted_list

        return {
            "labels": [item.get("Country Name", "N/A") for item in top_n],
            "values": [float(item[year]) for item in top_n],
            "metadata": {
                "year": year,
                "operation": op,
                "count": len(top_n)
            }
        }