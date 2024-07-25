import psycopg2
import logging
from sqlalchemy import create_engine, Column, String, Numeric, Integer, DateTime
from sqlalchemy.orm import declarative_base, aliased
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

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

def _get_db_session():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def _close_session(session):
    session.close()

def get_all_bins() -> list[Bin]:
    session = _get_db_session()

    latest_observations = []
    try:
        bin_alias = aliased(Bin)
        subquery = (
            session.query(
                bin_alias.dev_id,
                func.max(bin_alias.time).label('latest_timestamp')
            )
            .group_by(bin_alias.dev_id)
            .subquery()
        )
        latest_observations = (
            session.query(Bin)
            .join(subquery,
                  (Bin.dev_id == subquery.c.dev_id) & 
                  (Bin.time == subquery.c.latest_timestamp))
            .all()
        )
    except Exception as e:
        logger.error("Error retrieving data on 'bins' table", e)
    finally:
        _close_session(session=session)
        return latest_observations

def get_bin_prediction(dev_id: str) -> float:
    session = _get_db_session()

    fill_level = -1
    try:
        bin = session.query(Bin)\
            .filter_by(dev_id=dev_id)\
            .order_by(Bin.time.desc())\
            .first()
        fill_level = bin.fill_level if bin is not None else -1
    except Exception as e:
        logger.error(f"Error retrieving prediction for bin with dev_id={dev_id}", e)
    finally:
        _close_session(session=session)
        return fill_level