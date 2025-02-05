from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import glob
from datetime import datetime

DATABASE_URL = "mysql+mysqlconnector://root:@localhost/research_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Blinks(Base):
    __tablename__ = "blinks"
    id = Column(Integer, primary_key=True, index=True)
    trial_number = Column(Integer, nullable=False)
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
    df.fillna(value=None, inplace=True)

    for _, row in df.iterrows():
        entry = table(candidate_number =candidate_number, trial_number=trial_number, **row.to_dict())
        session.add(entry)
    
    session.commit()
    session.close()
