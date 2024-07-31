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
    sensor_name = Column(String(50), nullable=False)
    latitude = Column(Numeric, nullable=True)
    longitude = Column(Numeric, nullable=True)

class Weather(Base):
    __tablename__ = 'weather'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dev_id = Column(String(50), nullable=False)
    sensor_name = Column(String(50), nullable=False)
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

def _get_db_session(uri: str):
    engine = create_engine(uri)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def _close_session(session):
    session.close()

def bin_is_valid(bin):
    return 'dev_id' in bin and 'sensor_name' in bin and bin['dev_id'] != '' and bin['sensor_name'] != ''

def bin_not_present(session, dev_id: str) -> bool:
    return session.query(Bin)\
        .filter_by(dev_id=dev_id)\
        .count() == 0

def weather_is_valid(weather):
    return 'dev_id' in weather and 'sensor_name' in weather and weather['dev_id'] != '' and weather['sensor_name'] != ''

def weather_not_present(session, dev_id: str) -> bool:
    return session.query(Weather)\
        .filter_by(dev_id=dev_id)\
        .count() == 0

def insert_bins(bin_data):
    session = _get_db_session(uri=DATABASE_URI)
    new_bins = []
    for bin in bin_data:
        if bin_is_valid(bin) and bin_not_present(session, bin['dev_id']):
            new_bins.append(
                Bin(dev_id=bin['dev_id'],
                    sensor_name=bin['sensor_name'],
                    latitude=bin['lat_long']['lat'] if bin['lat_long'] is not None else None,
                    longitude=bin['lat_long']['lon'] if bin['lat_long'] is not None else None)
            )
    try:
        session.add_all(new_bins)
        session.commit()
        logger.info(f"Saved {len(new_bins)} new bins data on postgres")
    except Exception as e:
        logger.error("Error saving new data on 'bins' table", e)
    finally:
        _close_session(session=session)

def insert_weather(weather_data):
    session = _get_db_session(uri=DATABASE_URI)
    new_weather = []
    for weather in weather_data:
        if weather_is_valid(weather) and weather_not_present(session=session, dev_id=weather['dev_id']):
            new_weather.append(
                Weather(dev_id=weather['dev_id'],
                        sensor_name=weather['sensor_name'],
                        time=weather['time'],
                        battery=weather['battery'],
                        precipitation=weather['precipitation'],
                        air_temp=weather['airtemp'],
                        latitude=weather['lat_long']['lat'] if weather['lat_long'] is not None else None,
                        longitude=weather['lat_long']['lon'] if weather['lat_long'] is not None else None,
                        wind_speed=weather['windspeed'],
                        wind_direction=weather['winddirection'],
                        gust_speed=weather['gustspeed'],
                        vapour_pressure=weather['vapourpressure'],
                        atmospheric_pressure=weather['atmosphericpressure'],
                        relative_humidity=weather['relativehumidity'])
            )
    try:
        session.add_all(new_weather)
        session.commit()
        logger.info(f"Saved {len(new_weather)} new weather data on postgres")
    except Exception as e:
        logger.error("Error saving new data on 'weather' table", e)
    finally:
        _close_session(session=session)