from fastapi import FastAPI, Response, status, Depends
from sqlalchemy import create_engine, Column, Float, DateTime
from sqlalchemy.orm import declarative_base, Session, sessionmaker
import requests
from typing import List
import pandas as pd
from pydantic import BaseModel
from datetime import datetime


app = FastAPI()

# Database setup
DATABASE_URL = "sqlite:///./weather_data.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class WeatherData(Base):
    __tablename__ = "weather_data"
    id = Column(DateTime, primary_key=True, index=True)
    temperature_2m = Column(Float)
    relative_humidity_2m = Column(Float)
    surface_pressure = Column(Float)
    vapour_pressure_deficit = Column(Float)
    wind_speed_10m = Column(Float)
    soil_temperature_0cm = Column(Float)

class WeatherDataSchema(BaseModel):
    id: datetime
    temperature_2m: float
    relative_humidity_2m: float
    surface_pressure: float
    vapour_pressure_deficit: float
    wind_speed_10m: float
    soil_temperature_0cm: float

    class Config:
        from_attributes = True


Base.metadata.create_all(bind=engine)


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
    """ now = datetime.now(pytz.UTC)
    hourly_df["time_diff"] = abs(hourly_df["date"] - now)
    closest_data = hourly_df.loc[hourly_df["time_diff"].idxmin()] """
    # Save to database
    db = SessionLocal()
    try:
        for _, row in hourly_df.iterrows():
            weather_record = WeatherData(
                id=row["date"],
                temperature_2m=row["temperature_2m"],
                relative_humidity_2m=row["relative_humidity_2m"],
                surface_pressure=row["surface_pressure"],
                vapour_pressure_deficit=row["vapour_pressure_deficit"],
                wind_speed_10m=row["wind_speed_10m"],
                soil_temperature_0cm=row["soil_temperature_0cm"]
            )
            db.merge(weather_record)  
        db.commit()
    finally:
        db.close()

@app.get("/collect-weather-data")
def collect_weather_data():
    fetch_weather_data()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.get("/weather-data", response_model=List[WeatherDataSchema])
def get_weather_data(db: Session = Depends(get_db)):
    data = db.query(WeatherData).all()
    return data

@app.delete("/clear-weather-data", status_code=status.HTTP_204_NO_CONTENT)
def clear_weather_data(db: Session = Depends(get_db)):
    db.query(WeatherData).delete()  # Delete all records in the WeatherData table
    db.commit()
