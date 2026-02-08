from functools import reduce

def is_valid_gdp(value):
    return value and str(value).strip() != "" and str(value).replace('.', '', 1).isdigit()


def process_data(data, config):

    target_field = config.get("target_field")
    if data and target_field not in data[0]:
        if "Region" in data[0]:
            target_field = "Region"
        elif "Continent" in data[0]:
            target_field = "Continent"


    target_value = config.get("target_value")
    target_year = str(config.get("year"))
    operation = config.get("operation", "average").lower()

    #validation
    if not data or target_year not in data[0]:
        return {"error": "Invalid Data or Year not found"}


    #if looking for a country, we find specific row
    #and if looking for a region we need rows that match region and have data for the target year
    if target_field == "Country Name":
        filtered_data = [row for row in data if row.get("Country Name") == target_value]
    else:
        filtered_data = [row for row in data if row.get(target_field) == target_value and is_valid_gdp(row.get(target_year))]

    if not filtered_data:
        return {"error": "No data found"}

    country_names = []
    gdp_values = []

    if target_field == "Country Name":
        #logic for single country is to extract all year
        row = filtered_data[0]
        #all the keys are digits
        years = [k for k in row.keys() if k.isdigit() and is_valid_gdp(row[k])]
        country_names = years # X-axis will be Years
        gdp_values = [float(row[y]) for y in years] # Y-axis will be GDP
    else:
        #logic for region extract sepcific year for all countries
        country_names = [row["Country Name"] for row in filtered_data]
        gdp_values = [float(row[target_year]) for row in filtered_data]

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

