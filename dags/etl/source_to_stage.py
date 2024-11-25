import os
import pandas as pd
from sqlalchemy import text
from config.config import SOURCE_DIR
from etl.db_utils import get_db_session, close_session
from etl.models import StateAQIStage, USCountiesStage, Metadata


def set_cet(table_name):
    session = get_db_session()
    try:
        session.query(Metadata).filter(Metadata.table_name == table_name).update({"cet": pd.Timestamp.utcnow()})
        session.commit()
        print(f"Metadata CET updated for {table_name}")
    finally:
        close_session()


def set_lset(table_name):
    session = get_db_session()
    try:
        session.query(Metadata).filter(Metadata.table_name == table_name).update({"lset": pd.Timestamp.utcnow()})
        session.commit()
        print(f"Metadata LSET updated for {table_name}")
    finally:
        close_session()

def truncate_table(table_name):
    session = get_db_session()
    try:
        session.execute(text(f"TRUNCATE TABLE {table_name}"))
        session.commit()
        print(f"Truncated table {table_name}.")
    finally:
        close_session()

def get_metadata(table_name):
    session = get_db_session()
    try:
        metadata = session.query(Metadata).filter(Metadata.table_name == table_name).first()
        if metadata:
            return metadata.cet, metadata.lset
        return None, None
    finally:
        close_session()

def process_aqi_files(table_name):
    cet, lset = get_metadata(table_name)
    print(f"LSET: {lset}, CET: {cet} for {table_name}")
    for filename in os.listdir(SOURCE_DIR):
        if filename.startswith("10_state_aqi_") and filename.endswith(".csv"):
            file_path = os.path.join(SOURCE_DIR, filename)
            df = pd.read_csv(file_path)

            df = df.rename(columns={
                'State Name': 'state_name',
                'county Name': 'county_name',
                'State Code': 'state_code',
                'County Code': 'county_code',
                'Date': 'measured_date',
                'AQI': 'aqi_value',
                'Category': 'aqi_category',
                'Defining Parameter': 'defining_parameter',
                'Defining Site': 'defining_site',
                'Number of Sites Reporting': 'num_of_sites_reporting',
                'Created': 'created',
                'Last Updated': 'last_updated'
            })

            df['measured_date'] = pd.to_datetime(df['created']).dt.date
            df['created'] = pd.to_datetime(df['created'])  
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            df = df[(df['last_updated'] >= lset) & (df['last_updated'] <= cet)]
            df['aqi_category'] = df['aqi_value'].apply(modify_category) 
            df['county_name'] = df['county_name'].str.strip()   

            session = get_db_session()
            try:
                for _, row in df.iterrows():
                    record = StateAQIStage(**row.to_dict())
                    session.add(record)
                session.commit()
            finally:
                close_session()
            print(f"Loaded data from {filename} into {table_name}.")

def process_counties_file(table_name):
    counties_file = os.path.join(SOURCE_DIR, "uscounties.csv")
    print("File exists:", os.path.exists(counties_file))  # This should print True if the file exists
    if os.path.exists(counties_file):
        df = pd.read_csv(counties_file)
        df =df.rename(columns={
            'county': 'county_name',
            'county_full': 'county_fullname',
            'lat': 'latitude',
            'lng': 'longitude',
            'population': 'county_population'
        })
        df['county_name'] = df['county_name'].str.strip() 
        session = get_db_session()
        try:
            for _, row in df.iterrows():
                record = USCountiesStage(**row.to_dict())
                session.add(record)
            session.commit()
        finally:
            close_session()
        print(f"Loaded data from {counties_file} into {table_name}.")

def modify_category(aqi):
    """Categorize AQI values based on given thresholds."""
    if 0 <= aqi <= 50:
        return "Good"
    elif 51 <= aqi <= 100:
        return "Moderate"
    elif 101 <= aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    elif 151 <= aqi <= 200:
        return "Unhealthy"
    elif 201 <= aqi <= 300:
        return "Very Unhealthy"
    elif aqi > 300:
        return "Hazardous"
    else:
        return "Unknown"  # Default for any unexpected AQI values
