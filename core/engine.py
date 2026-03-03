# core/engine.py
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
        
        op = self.config.get("operation", "")
        op_clean = str(op).strip().lower()
        
        r_type = self.config.get("region_type")
        r_val = self.config.get("region_value")

        # Smart fallback: Fix config if region_type is mistakenly passed as the continent name
        if r_type in ["Asia", "Europe", "Africa", "North America", "South America", "Oceania"] and not r_val:
            r_val = r_type
            r_type = "Continent"

        # determine years_range for multi-year operations
        years_range = []
        if start_year and end_year:
            years_range = [str(y) for y in range(int(start_year), int(end_year) + 1)]

        # determine if operation relies on a single year or a range
        multi_year_ops = [
            "gdp_growth_rate", "global_gdp_trend",
            "fastest_growing_continent", "consistent_decline"
        ]
        is_multi_year = op_clean in multi_year_ops
        
        # Operations that must evaluate ALL continents to work correctly
        global_ops = ["avg_gdp_by_continent", "fastest_growing_continent", "continent_contribution"]

        # filter data by region and data availability
        filtered = []
        for row in raw_data:
            # Check region match, but bypass for operations that need to process all continents
            region_match = True
            if r_type and r_val and op_clean not in global_ops:
                region_match = (row.get(r_type) == r_val)
            
            if not region_match:
                continue
                
            # --- CRITICAL FIX ---
            # Do NOT filter out missing data for avg_gdp_by_continent yet! 
            # We must pass all rows through so we can count the TRUE total number of countries.
            if op_clean == "avg_gdp_by_continent":
                pass 
            elif is_multi_year:
                if years_range and not any(self._has_numeric(row, y) for y in years_range):
                    continue
            else: 
                if not self._has_numeric(row, str(year)):
                    continue
                    
            filtered.append(row)

        if not filtered:
            self.sink.write({"error": "No data matches criteria", "data": []})
            return

        # Execute transformation 
        results = self._transform(filtered, op_clean, str(year), years_range, decline_years)
        
        # Restore original operation name for outputs
        if "metadata" in results:
            results["metadata"]["operation"] = op
            
        self.sink.write(results)

    def _has_numeric(self, row: Dict[str, Any], year: str) -> bool:
        try:
            val = row.get(year)
            return val is not None and str(val).strip() != "" and str(val).lower() != "nan"
        except (ValueError, TypeError):
            return False

    def _is_valid_continent(self, cont: Any) -> bool:
        """Helper to exclude aggregate dataset rows posing as continents."""
        if not cont:
            return False
        c = str(cont).strip().lower()
        return c not in ["global", "world", "unknown", "none", ""]

    def _transform(self, data: List[Dict], op: str, year: str, years_range: List[str], decline_years: int) -> Dict[str, Any]:
        # --- Top 10 (Specific Year) ---
        if op == "top_10":
            sorted_list = sorted(data, key=lambda x: float(x[year]), reverse=True)
            return self._format_simple(sorted_list[:10], year, op)

        # --- Bottom 10 (Specific Year) ---
        if op == "bottom_10":
            sorted_list = sorted(data, key=lambda x: float(x[year]))
            return self._format_simple(sorted_list[:10], year, op)

        # --- GDP Growth Rate (Period Range) ---
        if op == "gdp_growth_rate":
            growth = []
            start_y, end_y = years_range[0], years_range[-1]
            for row in data:
                if not self._is_valid_continent(row.get("Continent")):
                    continue
                if self._has_numeric(row, start_y) and self._has_numeric(row, end_y):
                    start_val, end_val = float(row[start_y]), float(row[end_y])
                    if start_val > 0:
                        rate = ((end_val - start_val) / start_val) * 100
                        growth.append({"country": row.get("Country Name", "N/A"), "growth": rate})
            growth_sorted = sorted(growth, key=lambda x: x["growth"], reverse=True)
            return {
                "labels": [g["country"] for g in growth_sorted],
                "values": [g["growth"] for g in growth_sorted],
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Average GDP by Continent (Strict Division, Single Year) ---
        if op == "avg_gdp_by_continent":
            cont_totals = {}
            cont_counts = {}
            
            for row in data:
                cont = str(row.get("Continent", "")).strip()
                country_name = str(row.get("Country Name", "")).strip()
                
                # Drop invalid continents
                if not self._is_valid_continent(cont):
                    continue
                
                # Exclude Global/Regional summary rows posing as a country
                if country_name.lower() in ["world", "global", "asia", "europe", "africa", "north america", "south america", "oceania", "unknown"]:
                    continue

                # Count this country toward the continent total regardless of whether it has data this year!
                cont_counts[cont] = cont_counts.get(cont, 0) + 1
                
                # Only sum the GDP if the country actually has numeric data for this year
                if self._has_numeric(row, year):
                    cont_totals[cont] = cont_totals.get(cont, 0.0) + float(row[year])
            
            labels, values = [], []
            for cont, count in cont_counts.items():
                if count > 0:
                    labels.append(cont)
                    # Guaranteed strict division by ALL valid countries in that continent
                    avg_gdp = cont_totals.get(cont, 0.0) / float(count)
                    values.append(avg_gdp)
                    
            return {
                "labels": labels,
                "values": values,
                "metadata": {"operation": op, "year": year}
            }

        # --- Total Global GDP Trend (Period Range) ---
        if op == "global_gdp_trend":
            start_y, end_y = years_range[0], years_range[-1]
            valid_rows = [r for r in data if self._is_valid_continent(r.get("Continent", ""))]
            
            total_start = sum(float(row[start_y]) for row in valid_rows if self._has_numeric(row, start_y))
            total_end = sum(float(row[end_y]) for row in valid_rows if self._has_numeric(row, end_y))
            
            trend_pct = 0.0
            if total_start > 0:
                trend_pct = ((total_end - total_start) / total_start) * 100
                
            return {
                "labels": [f"Global Trend ({start_y}-{end_y})"],
                "values": [trend_pct],
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Fastest Growing Continent (Period Range) ---
        if op == "fastest_growing_continent":
            cont_growth = {}
            start_y, end_y = years_range[0], years_range[-1]
            
            continents = set(str(row.get("Continent", "")).strip() for row in data)
            valid_conts = [c for c in continents if self._is_valid_continent(c)]
            
            for cont in valid_conts:
                rows = [r for r in data if str(r.get("Continent", "")).strip() == cont]
                total_start = sum(float(r[start_y]) for r in rows if self._has_numeric(r, start_y))
                total_end = sum(float(r[end_y]) for r in rows if self._has_numeric(r, end_y))
                if total_start > 0:
                    cont_growth[cont] = ((total_end - total_start) / total_start) * 100
            
            if cont_growth:
                best = max(cont_growth.items(), key=lambda x: x[1])
                return {
                    "labels": [best[0]],
                    "values": [best[1]],
                    "metadata": {"operation": op, "years_range": years_range}
                }
            return {"labels": [], "values": [], "metadata": {"operation": op, "years_range": years_range}}

        # --- Countries with Consistent GDP Decline (Specified Period) ---
        if op == "consistent_decline":
            declining_labels, declining_values = [], []
            for row in data:
                if not self._is_valid_continent(row.get("Continent")):
                    continue
                
                vals = [float(row[y]) for y in years_range if self._has_numeric(row, y)]
                if len(vals) == len(years_range) and len(vals) > 1:
                    if all(x > y for x, y in zip(vals, vals[1:])):
                        declining_labels.append(row.get("Country Name", "N/A"))
                        declining_values.append(vals[-1])  
            return {
                "labels": declining_labels,
                "values": declining_values,
                "metadata": {"operation": op, "years_range": years_range}
            }

        # --- Contribution of Each Continent to Global GDP (Single Year) ---
        if op == "continent_contribution":
            cont_totals = {}
            global_total = 0
            for row in data:
                cont = str(row.get("Continent", "")).strip()
                if not self._is_valid_continent(cont):
                    continue
                    
                if self._has_numeric(row, year):
                    val = float(row[year])
                    cont_totals[cont] = cont_totals.get(cont, 0) + val
                    global_total += val
                        
            contributions = {c: (v / global_total) * 100 for c, v in cont_totals.items()} if global_total > 0 else {}
            sorted_contribs = sorted(contributions.items(), key=lambda item: item[1], reverse=True)
            
            return {
                "labels": [item[0] for item in sorted_contribs],
                "values": [item[1] for item in sorted_contribs],
                "metadata": {"operation": op, "year": year}
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