import os
import pandas as pd
from db_connection import DatabaseConnector
 
# Dateipfade zur CSV-Dateien festlegen
base_dir = os.path.dirname(os.path.dirname(__file__))
csv_dir = os.path.join(base_dir, "data", "raw")
 
forecast_file = os.path.join(csv_dir, "historical_forecast_data.csv")
weather_file = os.path.join(csv_dir, "historical_weather_data.csv")
 
 
def load_and_insert_data(engine):
    """
    Lädt CSV-Dateien und fügt die Daten in die Tabellen ein.
    """
    try:
        # --- Historische Vorhersagedaten ---
        print("Einfügen der historischen Vorhersagedaten...")
        forecast_df = pd.read_csv(forecast_file)
 
        # NaN in None konvertieren
        forecast_df = forecast_df.where(pd.notna(forecast_df), None)
 
        # Daten in die Tabelle einfügen
        forecast_df.to_sql("historical_forecast", con=engine, if_exists="append", index=False)
        print("Daten in 'historical_forecast' eingefügt!")
        # --- Historische Wetterdaten ---
        print("Einfügen der historischen Wetterdaten...")
        weather_df = pd.read_csv(weather_file)
 
        # NaN in None konvertieren
        weather_df = weather_df.where(pd.notna(weather_df), None)
 
        # Daten in die Tabelle einfügen
        weather_df.to_sql("historical_weather", con=engine, if_exists="append", index=False)
        print("Daten in 'historical_weather' eingefügt!")
 
    except Exception as e:
        print(f"Fehler beim Einfügen: {e}")
 
 
def main():
    # Verbindung zur Datenbank herstellen
    db_connector = DatabaseConnector()
    engine = db_connector.get_engine()
 
    if engine is not None:
        try:
            load_and_insert_data(engine)
            print("Daten erfolgreich eingefügt!")
        except Exception as e:
            print(f"Fehler beim Einfügen der Daten: {e}")
        finally:
            print("Vorgang abgeschlossen.")
    else:
        print("Verbindung zur Datenbank fehlgeschlagen.")
 
 
if __name__ == "__main__":
    main()