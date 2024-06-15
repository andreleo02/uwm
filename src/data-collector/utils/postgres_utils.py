import psycopg2
import logging
from sqlalchemy import create_engine, Column, String, Numeric, Integer, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

user = 'postgres'
password = 'password'
host = 'postgres'
database = 'db_waste'
port = '5432'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

DATABASE_URI = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

Base = declarative_base()

class Bin(Base):
    __tablename__ = 'bins'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dev_id = Column(String(50), nullable=False)
    time = Column(DateTime, nullable=False)
    battery = Column(Numeric, nullable=True)
    fill_level = Column(Numeric, nullable=False)
    temperature = Column(Numeric, nullable=False)
    latitude = Column(Numeric, nullable=True)
    longitude = Column(Numeric, nullable=True)

class Weather(Base):
    __tablename__ = 'weather'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dev_id = Column(String(50), nullable=False)
    time = Column(DateTime, nullable=False)
    battery = Column(Numeric, nullable=True)
    air_temp = Column(Numeric, nullable=False)
    latitude = Column(Numeric, nullable=True)
    longitude = Column(Numeric, nullable=True)
    precipitation = Column(Numeric, nullable=True)
    wind_speed = Column(Numeric, nullable=True)
    wind_direction = Column(Numeric, nullable=True)
    gust_speed = Column(Numeric, nullable=True)
    vapour_pressure = Column(Numeric, nullable=True)
    atmospheric_pressure = Column(Numeric, nullable=True)
    relative_humidity = Column(Numeric, nullable=True)

def get_db_session(uri: str):
    engine = create_engine(uri)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def close_session(session):
    session.close()

def bin_is_valid(bin):
    valid = 'dev_id' in bin and 'time' in bin and 'temperature' in bin and 'fill_level' in bin and 'battery' in bin
    valid = valid and (bin['fill_level'] >= 0 and bin['fill_level'] <= 100) and (bin['battery'] >= 0 and bin['battery'] <= 100)
    return valid

def weather_is_valid(weather):
    valid = 'dev_id' in weather and 'time' in weather and 'air_temp' in weather and 'battery' in weather
    valid = valid and (weather['battery'] >= 0 and weather['battery'] <= 100)
    return valid

def insert_bins(bin_data):
    session = get_db_session(uri=DATABASE_URI)
    new_bins = []
    for bin in bin_data:
        if bin_is_valid(bin):
            new_bins.append(
                Bin(dev_id=bin['dev_id'],
                    time=bin['time'],
                    battery=bin['battery'],
                    fill_level=bin['fill_level'],
                    temperature=bin['temperature'],
                    latitude=bin['latitude'] if 'latitude' in bin else None,
                    longitude=bin['longitude'] if 'longitude' in bin else None)
            )
    try:
        session.add_all(new_bins)
        session.commit()
        logger.info(f"Saved new {len(new_bins)} bins data on postgres")
    except Exception as e:
        logger.error("Error saving new data on 'bins' table", e)
    finally:
        close_session(session=session)

def insert_weather(weather_data):
    session = get_db_session(uri=DATABASE_URI)
    new_weather = []
    for weather in weather_data:
        if weather_is_valid(weather):
            new_weather.append(
                Weather(dev_id=weather['dev_id'],
                        time=weather['time'],
                        battery=weather['battery'],
                        precipitation=weather['precipitation'],
                        air_temp=weather['air_temp'],
                        latitude=weather['latitude'],
                        longitude=weather['longitude'],
                        wind_speed=weather['wind_speed'],
                        wind_direction=weather['wind_direction'],
                        gust_speed=weather['gust_speed'],
                        vapour_pressure=weather['vapour_pressure'],
                        atmospheric_pressure=weather['atmospheric_pressure'],
                        relative_humidity=weather['relative_humidity'])
            )
    try:
        session.add_all(new_weather)
        session.commit()
        logger.info(f"Saved new {len(new_weather)} weather data on postgres")
    except Exception as e:
        logger.error("Error saving new data on 'weather' table", e)
    finally:
        close_session(session=session)