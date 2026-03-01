from typing import List, Dict, Any
from core.contracts import DataSink

class GDPEngine:
    def __init__(self, sink: DataSink, config: Dict[str, Any]):
        self.sink = sink
        self.config = config

    def execute(self, raw_data: List[Dict[str, Any]]) -> None:
        # read config fields safely
        year = self.config.get("year")
        start_year = self.config.get("start_year")
        end_year = self.config.get("end_year")
        decline_years = int(self.config.get("decline_years", 5))
        op = self.config.get("operation")
        r_type = self.config.get("region_type")
        r_val = self.config.get("region_value")

        # determine years_range for multi-year operations
        years_range = []
        if start_year and end_year:
            years_range = [str(y) for y in range(int(start_year), int(end_year) + 1)]

        # determine if operation relies on a single year or a range
        multi_year_ops = [
            "gdp_growth_rate", "avg_gdp_by_continent", "global_gdp_trend",
            "fastest_growing_continent", "consistent_decline", "continent_contribution"
        ]
        is_multi_year = op in multi_year_ops

        # filter data by region and data availability
        filtered = []
        for row in raw_data:
            # Check region match
            region_match = True
            if r_type and r_val:
                region_match = (row.get(r_type) == r_val)
            
            if not region_match:
                continue
                
            # Check if required data exists based on operation type
            if is_multi_year:
                # Need at least some data in the year range
                if years_range and not any(self._has_numeric(row, y) for y in years_range):
                    continue
            else:
                # Need exact year data
                if not self._has_numeric(row, str(year)):
                    continue
                    
            filtered.append(row)

        if not filtered:
            self.sink.write({"error": "No data matches criteria", "data": []})
            return

        # Execute transformation 
        results = self._transform(filtered, op, str(year), years_range, decline_years)
        self.sink.write(results)

    def _has_numeric(self, row: Dict[str, Any], year: str) -> bool:
        try:
            val = row.get(year)
            return val is not None and str(val).strip() != "" and str(val).lower() != "nan"
        except Exception:
            return False

    def _transform(self, data: List[Dict], op: str, year: str, years_range: List[str], decline_years: int) -> Dict[str, Any]:
        # --- Top 10 ---
        if op == "top_10":
            sorted_list = sorted(data, key=lambda x: float(x[year]), reverse=True)
            return self._format_simple(sorted_list[:10], year, op)

        # --- Bottom 10 ---
        if op == "bottom_10":
            sorted_list = sorted(data, key=lambda x: float(x[year]))
            return self._format_simple(sorted_list[:10], year, op)

        # --- GDP Growth Rate ---
        if op == "gdp_growth_rate":
            growth = []
            for row in data:
                vals = [float(row[y]) for y in years_range if self._has_numeric(row, y)]
                if len(vals) >= 2:
                    rate = ((vals[-1] - vals[0]) / vals[0]) * 100
                    growth.append({"country": row.get("Country Name", "N/A"), "growth": rate})
            growth_sorted = sorted(growth, key=lambda x: x["growth"], reverse=True)
            return {
                "labels": [g["country"] for g in growth_sorted],
                "values": [g["growth"] for g in growth_sorted],
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Average GDP by Continent ---
        if op == "avg_gdp_by_continent":
            cont_map = {}
            for row in data:
                cont = row.get("Continent", "Unknown")
                vals = [float(row[y]) for y in years_range if self._has_numeric(row, y)]
                if vals:
                    cont_map.setdefault(cont, []).append(sum(vals) / len(vals))
            final_avg = {c: sum(v) / len(v) for c, v in cont_map.items()}
            return {
                "labels": list(final_avg.keys()),
                "values": list(final_avg.values()),
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Total Global GDP Trend ---
        if op == "global_gdp_trend":
            trend = {}
            for y in years_range:
                total = sum(float(row[y]) for row in data if self._has_numeric(row, y))
                trend[y] = total
            return {
                "labels": list(trend.keys()),
                "values": list(trend.values()),
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Fastest Growing Continent ---
        if op == "fastest_growing_continent":
            cont_growth = {}
            for cont in set(row.get("Continent", "Unknown") for row in data):
                rows = [r for r in data if r.get("Continent") == cont]
                total_start = sum(float(r[years_range[0]]) for r in rows if self._has_numeric(r, years_range[0]))
                total_end = sum(float(r[years_range[-1]]) for r in rows if self._has_numeric(r, years_range[-1]))
                if total_start > 0:
                    cont_growth[cont] = ((total_end - total_start) / total_start) * 100
            best = max(cont_growth.items(), key=lambda x: x[1], default=("None", 0))
            return {
                "labels": [best[0]],
                "values": [best[1]],
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Countries with Consistent GDP Decline ---
        if op == "consistent_decline":
            declining_labels = []
            declining_values = []
            
            # Fetch the specific sequence of decline years safely
            if years_range and len(years_range) >= decline_years:
                decline_years_list = years_range[-decline_years:]
            else:
                end_yr = int(years_range[-1]) if years_range else int(self.config.get("end_year", year))
                decline_years_list = [str(y) for y in range(end_yr - decline_years + 1, end_yr + 1)]

            for row in data:
                vals = [float(row[y]) for y in decline_years_list if self._has_numeric(row, y)]
                if len(vals) == decline_years and all(x > y for x, y in zip(vals, vals[1:])):
                    declining_labels.append(row.get("Country Name", "N/A"))
                    declining_values.append(vals[-1])  # Yield the most recent GDP instead of 0
                    
            return {
                "labels": declining_labels,
                "values": declining_values,
                "metadata": {"operation": op, "decline_years": decline_years, "years_range": decline_years_list}
            }

        # --- Contribution of Each Continent to Global GDP ---
        if op == "continent_contribution":
            cont_totals = {}
            global_total = 0
            for row in data:
                cont = row.get("Continent", "Unknown")
                for y in years_range:
                    if self._has_numeric(row, y):
                        val = float(row[y])
                        cont_totals[cont] = cont_totals.get(cont, 0) + val
                        global_total += val
                        
            # Prevent Division by Zero
            contributions = {c: (v / global_total) * 100 for c, v in cont_totals.items()} if global_total > 0 else {}
            
            # Sort contributions highest to lowest for better output presentation
            sorted_contribs = sorted(contributions.items(), key=lambda item: item[1], reverse=True)
            
            return {
                "labels": [item[0] for item in sorted_contribs],
                "values": [item[1] for item in sorted_contribs],
                "metadata": {"operation": op, "years_range": years_range}
            }

        # Fallback operation
        valid_rows = [row for row in data if self._has_numeric(row, year)]
        return {
            "labels": [row.get("Country Name", "N/A") for row in valid_rows],
            "values": [float(row[year]) for row in valid_rows],
            "metadata": {"operation": op, "year": year}
        }

    def _format_simple(self, data: List[Dict], year: str, op: str) -> Dict[str, Any]:
        return {
            "labels": [row.get("Country Name", "N/A") for row in data],
            "values": [float(row[year]) for row in data],
            "metadata": {"operation": op, "year": year}
        }