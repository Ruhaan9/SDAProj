from functools import reduce

def process_data(data, config):

    target_field = config.get("target_field")
    target_value = config.get("target_value")
    target_year = str(config.get("year"))
    operation = config.get("operation", "average").lower()

    #validation
    if not data or target_year not in data[0]:
        return {"error": "Invalid Data or Year not found"}

    region_filter = lambda  row: (
            row.get(target_field) == target_value and 
            row.get(target_year, "").strip() != "" and
            row.get(target_year).replace('.', '',1).isdigit()
    )

    filtered_data = list(filter(region_filter, data))

    if not filtered_data:
        return {"error": "No data found"}

    extract_data = lambda row: (row["Country Name"],float(row[target_year]))

    #map returning tuples
    mapped_data = list(map(extract_data, filtered_data))

    #seperating into two lists for plotting using list comprehension
    country_names = [item[0] for item in mapped_data]
    gdp_values = [item[1] for item in mapped_data]

    count = len(gdp_values)
    total_gdp = reduce(lambda acc, x: acc + x, gdp_values, 0)

    result = 0
    if operation == "sum":
        result = total_gdp
    elif operation == "average":
        if count > 0:
            result = total_gdp / count
        else:
            result = 0

    return {
        "Region": target_value,
        "Year": target_year,
        "Operation": operation,
        "Result": result,
        "Count": count,
        "Country_Names": country_names,
        "Data_points": gdp_values
    }

