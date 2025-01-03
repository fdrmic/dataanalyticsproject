import os
import pandas as pd
from sqlalchemy import text
from db_connection import DatabaseConnector
 
 
# Dateipfade zur CSV-Dateien festlegen
base_dir = os.path.dirname(os.path.dirname(__file__))
csv_dir = os.path.join(base_dir, "data", "raw")
 
forecast_file = os.path.join(csv_dir, "historical_forecast_data.csv")
weather_file = os.path.join(csv_dir, "historical_weather_data.csv")
 
 
def create_tables():
    try:
        # Verbindung zur Datenbank über SQLAlchemy herstellen
        db_connector = DatabaseConnector()
        engine = db_connector.get_engine()  # Engine einmalig erstellen
        print("SQLAlchemy Engine erfolgreich erstellt!")
 
        # Header der CSV-Dateien einlesen
        forecast_header = pd.read_csv(forecast_file, nrows=0).columns.tolist()
        weather_header = pd.read_csv(weather_file, nrows=0).columns.tolist()
 
        # Tabellen erstellen basierend auf den Headern
        # Historische Wetterdaten
        weather_table = text(f"""
            CREATE TABLE IF NOT EXISTS historical_weather (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join([f'{col} FLOAT' if col != 'datetime' else f'{col} DATETIME' for col in weather_header])}
            ) ENGINE=InnoDB;
        """)
 
        # Historische Vorhersagedaten
        forecast_table = text(f"""
            CREATE TABLE IF NOT EXISTS historical_forecast (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join([f'{col} FLOAT' if col != 'datetime' else f'{col} DATETIME' for col in forecast_header])}
            ) ENGINE=InnoDB;
        """)
 
        # SQL-Befehle ausführen über dieselbe Verbindung
        with engine.connect() as connection:  # Verbindung mit bestehender Engine!
            connection.execute(weather_table)
            connection.execute(forecast_table)
 
        print("Tabellen erfolgreich erstellt!")
 
    except Exception as e:
        print(f"Fehler beim Erstellen der Tabellen: {e}")
 
 
if __name__ == "__main__":
    create_tables()
 