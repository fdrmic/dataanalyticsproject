from api_wrapper.api_client import APIClient
from api_wrapper.config import Config
from api_wrapper.data_handler import DataHandler
import pandas as pd
import os

def main():
    # Initialize API clients
    forecast_client = APIClient(base_url=Config.BASE_URL_HISTORICAL_FORECAST)
    historical_client = APIClient(base_url=Config.BASE_URL_HISTORICAL)

    # Define default coordinates and time range
    latitude = Config.DEFAULT_LATITUDE
    longitude = Config.DEFAULT_LONGITUDE
    start_date = "2024-01-01"
    end_date = "2024-12-31"

    # Create paths for CSV files
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "..", "data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    # === Retrieve historical forecast data ===
    print("Retrieving historical forecast data...")
    forecast_result = forecast_client.get_historical_data(
        latitude, longitude, start_date, end_date, Config.HISTORICAL_FORECAST_PARAMS
    )

    if "error" in forecast_result:
        print("Error retrieving historical forecast data:", forecast_result["error"])
    else:
        print("Historical forecast data successfully retrieved.")

        # Process data
        hourly_data = forecast_result.get('hourly', {})
        timestamps = hourly_data.get('time', [])

        # Create DataFrame with datetime
        max_length = len(timestamps)
        df_forecast = pd.DataFrame({"datetime": pd.to_datetime(timestamps)})

        # Dynamically add all variables
        for var in Config.HISTORICAL_FORECAST_VARIABLES:
            df_forecast[var] = hourly_data.get(var, [pd.NA] * max_length)

        # Clean and categorize data
        df_forecast = DataHandler.clean_data(df_forecast)
        df_forecast = DataHandler.handle_missing_values(df_forecast)

        # Ensure column order (datetime first)
        column_order = ['datetime'] + Config.HISTORICAL_FORECAST_VARIABLES
        df_forecast = df_forecast[column_order]

        # Save CSV
        DataHandler.save_to_csv(df_forecast, os.path.join(output_dir, "historical_forecast_data.csv"))

    # === Retrieve actual historical weather data ===
    print("Retrieving actual historical weather data...")
    historical_result = historical_client.get_historical_data(
        latitude, longitude, start_date, end_date, Config.HISTORICAL_PARAMS
    )

    if "error" in historical_result:
        print("Error retrieving historical weather data:", historical_result["error"])
    else:
        print("Historical weather data successfully retrieved.")

        # Process data
        hourly_data = historical_result.get('hourly', {})
        timestamps = hourly_data.get('time', [])

        # Create DataFrame with datetime
        max_length = len(timestamps)
        df_historical = pd.DataFrame({"datetime": pd.to_datetime(timestamps)})

        # Dynamically add all variables
        for var in Config.HISTORICAL_VARIABLES:
            df_historical[var] = hourly_data.get(var, [pd.NA] * max_length)

        # Clean and categorize data
        df_historical = DataHandler.clean_data(df_historical)
        df_historical = DataHandler.handle_missing_values(df_historical)

        # Ensure column order (datetime first)
        column_order = ['datetime'] + Config.HISTORICAL_VARIABLES
        df_historical = df_historical[column_order]

        # Save CSV
        DataHandler.save_to_csv(df_historical, os.path.join(output_dir, "historical_weather_data.csv"))

if __name__ == "__main__":
    main()
