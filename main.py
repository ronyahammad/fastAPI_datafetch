# Import necessary libraries
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import pandas as pd

app = FastAPI()

# Database setup
DATABASE_URL = "sqlite:///./weather_data.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define SQLAlchemy model
class WeatherData(Base):
    __tablename__ = "weather_data"
    id = Column(DateTime, primary_key=True, index=True)
    temperature_2m = Column(Float)
    relative_humidity_2m = Column(Float)
    surface_pressure = Column(Float)
    vapour_pressure_deficit = Column(Float)
    wind_speed_10m = Column(Float)
    soil_temperature_0cm = Column(Float)

# Create tables
Base.metadata.create_all(bind=engine)

# Helper function to fetch and save weather data
def fetch_weather_data():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 36.9898,
        "longitude": 7.921066,
        "hourly": ["temperature_2m", "relative_humidity_2m", "surface_pressure", "vapour_pressure_deficit", "wind_speed_10m", "soil_temperature_0cm"],
        "timezone": "auto",
        "forecast_days": 1
    }
    response = requests.get(url, params=params)
    data = response.json()

    # Process and structure data
    hourly_data = {
        "date": pd.to_datetime(data["hourly"]["time"], utc=True),
        "temperature_2m": data["hourly"]["temperature_2m"],
        "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
        "surface_pressure": data["hourly"]["surface_pressure"],
        "vapour_pressure_deficit": data["hourly"]["vapour_pressure_deficit"],
        "wind_speed_10m": data["hourly"]["wind_speed_10m"],
        "soil_temperature_0cm": data["hourly"]["soil_temperature_0cm"],
    }
    hourly_df = pd.DataFrame(hourly_data)

    # Save to database
    db = SessionLocal()
    try:
        for index, row in hourly_df.iterrows():
            weather_record = WeatherData(
                id=row["date"],
                temperature_2m=row["temperature_2m"],
                relative_humidity_2m=row["relative_humidity_2m"],
                surface_pressure=row["surface_pressure"],
                vapour_pressure_deficit=row["vapour_pressure_deficit"],
                wind_speed_10m=row["wind_speed_10m"],
                soil_temperature_0cm=row["soil_temperature_0cm"]
            )
            db.merge(weather_record)  # `merge` to avoid duplicates on primary key
        db.commit()
    finally:
        db.close()

# FastAPI route to manually trigger data collection
@app.get("/collect-weather-data")
def collect_weather_data():
    fetch_weather_data()
    return {"status": "Data collected and saved to database."}