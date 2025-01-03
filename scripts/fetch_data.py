from api_wrapper.api_client import APIClient
from api_wrapper.config import Config
from api_wrapper.data_handler import DataHandler
import pandas as pd
import os


def main():
    # API-Clients initialisieren
    forecast_client = APIClient(base_url=Config.BASE_URL_HISTORICAL_FORECAST)
    historical_client = APIClient(base_url=Config.BASE_URL_HISTORICAL)

    # Standardkoordinaten und Zeitraum definieren
    latitude = Config.DEFAULT_LATITUDE
    longitude = Config.DEFAULT_LONGITUDE
    start_date = "2024-01-01"
    end_date = "2024-12-31"

    # Pfade für CSV-Dateien erstellen
    base_dir = os.path.dirname(os.path.abspath(__file__))  # Aktuelles Verzeichnis des Scripts
    output_dir = os.path.join(base_dir, "..", "data", "raw")  # Speicherort relativ zum Projektverzeichnis
    os.makedirs(output_dir, exist_ok=True)  # Verzeichnis erstellen, falls nicht vorhanden

    # === Abruf der historischen Vorhersagedaten ===
    print("Abruf der historischen Vorhersagedaten...")
    forecast_result = forecast_client.get_historical_data(
        latitude, longitude, start_date, end_date, Config.HISTORICAL_FORECAST_PARAMS
    )

    if "error" in forecast_result:
        print("Fehler beim Abrufen der historischen Vorhersagedaten:", forecast_result["error"])
    else:
        print("Historische Vorhersagedaten erfolgreich abgerufen.")

        # Daten verarbeiten
        hourly_data = forecast_result.get('hourly', {})
        timestamps = hourly_data.get('time', [])

        max_length = len(timestamps)
        df_forecast = pd.DataFrame({"datetime": pd.to_datetime(timestamps)})

        # Exakte Feldnamen aus Config verwenden
        for var in Config.HISTORICAL_FORECAST_VARIABLES:
            df_forecast[var] = hourly_data.get(var, [pd.NA] * max_length)

        # Datenbereinigung und Kategorien hinzufügen
        df_forecast = DataHandler.clean_data(df_forecast)
        df_forecast = DataHandler.handle_missing_values(df_forecast)

        # Spaltenreihenfolge sicherstellen
        df_forecast = df_forecast[Config.HISTORICAL_FORECAST_VARIABLES]

        # CSV speichern
        DataHandler.save_to_csv(df_forecast, os.path.join(output_dir, "historical_forecast_data.csv"))

    # === Abruf der tatsächlichen historischen Wetterdaten ===
    print("Abruf der tatsächlichen historischen Wetterdaten...")
    historical_result = historical_client.get_historical_data(
        latitude, longitude, start_date, end_date, Config.HISTORICAL_PARAMS
    )

    if "error" in historical_result:
        print("Fehler beim Abrufen der historischen Wetterdaten:", historical_result["error"])
    else:
        print("Historische Wetterdaten erfolgreich abgerufen.")

        # Daten verarbeiten
        hourly_data = historical_result.get('hourly', {})
        timestamps = hourly_data.get('time', [])

        max_length = len(timestamps)
        df_historical = pd.DataFrame({"datetime": pd.to_datetime(timestamps)})

        # Exakte Feldnamen aus Config verwenden
        for var in Config.HISTORICAL_VARIABLES:
            df_historical[var] = hourly_data.get(var, [pd.NA] * max_length)

        # Datenbereinigung und Kategorien hinzufügen
        df_historical = DataHandler.clean_data(df_historical)
        df_historical = DataHandler.handle_missing_values(df_historical)

        # Spaltenreihenfolge sicherstellen
        df_historical = df_historical[Config.HISTORICAL_VARIABLES]

        # CSV speichern
        DataHandler.save_to_csv(df_historical, os.path.join(output_dir, "historical_weather_data.csv"))


if __name__ == "__main__":
    main()
