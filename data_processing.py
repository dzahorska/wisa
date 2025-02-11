from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import glob
from datetime import datetime

DATABASE_URL = "mysql+mysqlconnector://root:@localhost/research_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

''' ORM for Blinks '''
class Blinks(Base):
    __tablename__ = "blinks"
    id = Column(Integer, primary_key=True, index=True)
    candidate_number = Column(Integer)
    trial_number = Column(String(255), nullable=False)
    section_id = Column(String(255), nullable=False)
    recording_id = Column(String(255), nullable=False)
    blink_id = Column(Integer, nullable=False)
    start_timestamp_ns = Column(BigInteger, nullable=False)
    end_timestamp_ns = Column(BigInteger, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    start_timestamp_converted = Column(DateTime, nullable=False)
        

Base.metadata.create_all(bind=engine, checkfirst=True)

def insert_data(file_path, candidate_number, trial_number, table):
    session = SessionLocal()
    df = pd.read_csv(file_path)

    '''Map the column names to an acceptable my SQL naming convention'''
    column_mapping = {
        "section id": "section_id",
        "recording id": "recording_id",
        "blink id": "blink_id",
        "start timestamp [ns]": "start_timestamp_ns",
        "end timestamp [ns]": "end_timestamp_ns",
        "duration [ms]": "duration_ms",
        "start timestamp [ns]_converted": "start_timestamp_converted"
    }
    df.rename(columns=column_mapping, inplace=True)
    
    # Remove extra columns that are not in the ORM model
    expected_columns = set(column.name for column in table.__table__.columns)
    df = df[list(expected_columns & set(df.columns))]
    
    df = df.replace({np.nan: None})

    print(candidate_number)
    
    for _, row in df.iterrows():
        entry = table(candidate_number=candidate_number, trial_number=trial_number, **row.to_dict())
        session.add(entry)
    
    session.commit()
    session.close()
