from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import glob
from datetime import datetime

from index import Base, SessionLocal


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

    column_mapping = {
        "section id": "section_id",
        "recording id": "recording_id",
        "blink id": "blink_id",
        "start timestamp [ns]": "start_timestamp_ns",
        "end timestamp [ns]": "end_timestamp_ns",
        "duration [ms]": "duration_ms",
        "start timestamp [ns]_converted": "start_timestamp_converted"
    }

    def insert_data(self, file_path, candidate_number, trial_number):
        session = SessionLocal()

        df = pd.read_csv(file_path)
        df.rename(columns=self.column_mapping, inplace=True)

        for _, row in df.iterrows():
            entry = Blinks(candidate_number=candidate_number, trial_number=trial_number, **row.to_dict())
            session.add(entry)
