import os
import csv
import requests
import pandas as pd
from io import StringIO

# folder paths
input_file = "./Resources/indicators.csv"
metadata_folder = "./Data/Indicators metadata"
data_folder = "./Data/Indicators data"

# Create folders if they don't exist
os.makedirs(metadata_folder, exist_ok=True)
os.makedirs(data_folder, exist_ok=True)

# Read indicators from the CSV file
def read_indicators(file_path):
    indicators = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            indicators.append(row[0])
    return indicators

# Function to perform the GET request to the API
def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def preprocess(data_folder):
    # Load indicator data
    income_distribution = pd.read_csv(os.path.join(data_folder, "Income distribution_ by deciles and area.csv"))
    national_income = pd.read_csv(os.path.join(data_folder, "National income National Saving at current prices.csv"))
    population_by_area = pd.read_csv(os.path.join(data_folder, "Population_ by geographic area and sex_.csv"))
    
    # Set list of countries, years, and deciles
    countries = income_distribution["Country__ESTANDAR"].unique()
    years = list(range(int(income_distribution["Years__ESTANDAR"].min()), int(income_distribution["Years__ESTANDAR"].max()) + 1))
    deciles = ["Decile " + str(i) for i in range(1, 10 + 1)]

    # Filter datasets to get the data of interest
    # Keep only national geographic records
    income_distribution = income_distribution[income_distribution["Geographical area"] == "National"]
    population_by_area = population_by_area[population_by_area["Geographical area"] == "National"]
    # Convert the value of the 'value' column from millions to thousands of dollars
    national_income = national_income[national_income["Income_saving"] == "Gross national income"]
    # Filter both genders in population by area
    population_by_area = population_by_area[population_by_area["Sex__ESTANDAR"] == "Both sexes"]
    # Convert percentage to value
    income_distribution["value"] = income_distribution["value"] / 100

    # Create the new dataset to store the result
    monthly_income_per_decil_by_country = []

    # Iterate over each country
    for country in countries:
        # Filter country data
        income_country = income_distribution[income_distribution["Country__ESTANDAR"] == country]
        national_income_country = national_income[national_income["Country__ESTANDAR"] == country]
        population_country = population_by_area[population_by_area["Country__ESTANDAR"] == country]

        # Iterate over each year
        for year in years:
            # Filter year data
            income_year = income_country[income_country["Years__ESTANDAR"] == year]
            national_income_year = national_income_country[national_income_country["Years__ESTANDAR"] == year]
            population_year = population_country[population_country["Years__ESTANDAR"] == year]

            # Check if the year is available in all three datasets
            if income_year.empty or national_income_year.empty or population_year.empty:
                continue  # If the year is missing in any of the datasets, move to the next year

            # Get corresponding values
            national_income_value = national_income_year["value"].values[0]
            population_value = population_year["value"].values[0]

            # Iterate over each decile
            for decile in deciles:
                income_decile = income_year[income_year["Deciles"] == decile]
                
                if income_decile.empty:
                    continue  # If decile information is not found, move to the next decile
                
                decile_value = income_decile["value"].values[0]

                # Calculate the average monthly income
                year_income = (decile_value * national_income_value) / (population_value * 0.1)
                monthly_income = round((year_income / 12), 2)

                # Store the result
                monthly_income_per_decil_by_country.append({
                    "iso3": income_year["iso3"].values[0],
                    "Country__ESTANDAR": country,
                    "Years__ESTANDAR": year,
                    "Monthly Income": monthly_income,
                    "Deciles": decile
                })

    # Convert the list of results into a DataFrame
    monthly_income_df = pd.DataFrame(monthly_income_per_decil_by_country)
    monthly_income_df["Monthly Income"] = monthly_income_df["Monthly Income"] * 1000
    # Save the results to a CSV file
    output_file = os.path.join(data_folder, "monthly_income_per_decil_by_country.csv")
    monthly_income_df.to_csv(output_file, index=False)
    
    print(f"Monthly income per decile calculation completed. Results saved to {output_file}")
    
# Save metadata and records to their respective files
def process_indicators(indicators, metadata_folder, data_folder):
    for indicator in indicators:
        print(f"Processing indicator {indicator}...")

        # Fetch metadata
        metadata_url = f"https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator}/metadata?lang=en&format=csv"
        metadata_csv = fetch_data_from_api(metadata_url)
        if metadata_csv:
            try:
                # Convert the received CSV into a DataFrame using StringIO
                metadata = pd.read_csv(StringIO(metadata_csv))
                # Metadata file name
                metadata_file_path = os.path.join(metadata_folder, f"Metadata ID {indicator}.xlsx")
                # Save the metadata to an Excel file
                metadata.to_excel(metadata_file_path, index=False, engine='openpyxl')
                print(f"Metadata saved for indicator {indicator}")

                # Get the indicator name
                indicator_name = metadata.loc[metadata['parameter'] == 'indicator_name', 'value'].values[0]
                if not indicator_name:
                    print(f"Indicator name missing for {indicator}. Skipping records fetch.")
                    continue

                # Fetch indicator records
                records_url = f"https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator}/records?lang=en&format=csv&members="
                records_csv = fetch_data_from_api(records_url)
                if records_csv:
                    try:
                        # Convert the records into a DataFrame
                        records = pd.read_csv(StringIO(records_csv))
                        # Records file name
                        safe_indicator_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in indicator_name)
                        records_file_path = os.path.join(data_folder, f"{safe_indicator_name}.csv")
                        # Save the records to a CSV file
                        records.to_csv(records_file_path, index=False)
                        print(f"Records saved for indicator {indicator} as {records_file_path}")
                    except Exception as e:
                        print(f"Error processing records for indicator {indicator}: {e}")

            except Exception as e:
                print(f"Error processing metadata for indicator {indicator}: {e}")
        else:
            print(f"Metadata fetch failed for indicator {indicator}. Skipping.")
