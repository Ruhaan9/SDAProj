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
    if "error" in results:
        print(f"Data Error: {results['error']}")
        return
# ^Data handling and validation

#display text statistics
    print("\n" + "="*40)
    print(f" GDP DASHBOARD: {results['Region']} ({results['Year']})")
    print("="*40)
    print(f" Operation : {results['Operation'].upper()}")
    print(f" Count     : {results['Count']}")
    print(f" Result    : ${results['Result']:,.2f}")
    print("="*40)

    #visualization
    countries = results['Country_Names']
    gdp_values = results['Data_points']

    if not gdp_values:
        print("No data points to plot")
        return


    # visualization
    print(f"Generating charts: {config.get('plot1_type')} & {config.get('plot2_type')}")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 7))

    # PLOT 1: country comparison
    plot1 = config.get("plot1_type", "bar").lower()

    if plot1 == "bar":
        ax1.bar(range(len(gdp_values)), gdp_values, color='skyblue')
        ax1.set_xticks(range(len(countries)))
        ax1.set_xticklabels(countries, rotation=90, fontsize=8)
    elif plot1 == "scatter":
        ax1.scatter(range(len(gdp_values)), gdp_values, color='purple', s=100)
        ax1.set_xticks(range(len(countries)))
        ax1.set_xticklabels(countries, rotation=90, fontsize=8)
    elif plot1 == "line":
        ax1.plot(range(len(gdp_values)), gdp_values, marker='o', color='green', linestyle='-')
        ax1.set_xticks(range(len(countries)))
        ax1.set_xticklabels(countries, rotation=90, fontsize=8)
    else:
        print(f"Warning: Unknown plot type '{plot1}'. Defaulting to Bar.")
        ax1.bar(range(len(gdp_values)), gdp_values, color='gray')
    
    context_label = "Region-wise" if config.get("target_field") in ["Region", "Continent"] else "Year-specific"
    ax1.set_title(f"{context_label} GDP Analysis ({plot1.title()})")

    ax1.set_ylabel("GDP (USD)")
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    #secondary plot
    plot2 = config.get("plot2_type", "histogram").lower()

    if plot2 == "histogram":
        ax2.hist(gdp_values, bins=10, color='salmon', edgecolor='black')
        ax2.set_xlabel("GDP Range")
        ax2.set_ylabel("Frequency")
    elif plot2 == "box":
        ax2.boxplot(gdp_values, vert=True, patch_artist=True)
        ax2.set_title("GDP Distribution (Box Plot)")
    elif plot2 == "line":
        ax2.plot(range(len(gdp_values)), gdp_values, marker='x', color='red')
        ax2.set_title("GDP Trend (Line Chart)")
    else:
        # default fallback 
        ax2.hist(gdp_values, bins=10, color='skyblue', edgecolor='black')
        ax2.set_title("GDP Distribution (Histogram)")


    plt.tight_layout()
    print("\nDisplaying charts:")
    plt.show()














