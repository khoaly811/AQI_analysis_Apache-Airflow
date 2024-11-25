import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from etl.db_utils import get_db_session, close_session
from etl.models import StateNDS, CountyNDS, MeasurementNDS, StateAQIStage, USCountiesStage

#Tìm kiếm dữ liệu đã có trong NDS chưa, nếu có thì cập nhật còn chưa thì thêm mới
def lookup_state_id(row):
    session = get_db_session()
    try:
        state_name = row.get('state_name') if not pd.isna(row.get('state_name')) else None
        state_code = row.get('state_code') if not pd.isna(row.get('state_code')) else None
        state_id = row.get('state_id') if not pd.isna(row.get('state_id')) else None

        existing_state = session.query(StateNDS).filter_by(state_name=state_name).first()
        if existing_state:
            existing_state.state_name = state_name
            existing_state.last_updated_nds = pd.Timestamp.utcnow()
        else:
            new_state = StateNDS(
                state_code=state_code,
                state_name=state_name,
                state_id=state_id,
                created_date_nds=pd.Timestamp.utcnow(),
                last_updated_nds=pd.Timestamp.utcnow(),
                source_id=1
            )
            session.add(new_state)
        session.commit()
    finally:
        close_session()

#Thêm dữ liệu vào bảng state_nds
def get_merged_state_data():
    session = get_db_session()
    try:
        state_aqi = session.query(StateAQIStage.state_name, StateAQIStage.state_code).distinct().all()
        us_counties = session.query(USCountiesStage.state_name, USCountiesStage.state_id).distinct().all()

        state_aqi_df = pd.DataFrame(state_aqi, columns=['state_name', 'state_code'])
        us_counties_df = pd.DataFrame(us_counties, columns=['state_name', 'state_id'])

        merged_df = pd.merge(us_counties_df, state_aqi_df, on=['state_name'], how='outer')
        merged_df.apply(lookup_state_id, axis=1)
    finally:
        close_session()

#Tìm kiếm dữ liệu đã có trong NDS chưa, nếu có thì cập nhật còn chưa thì thêm mới
def lookup_county_id(row):
    session = get_db_session()
    try:
        county_fips = row.get('county_fips') if not pd.isna(row.get('county_fips')) else None
        county_name = row.get('county_name') if not pd.isna(row.get('county_name')) else None
        county_fullname = row.get('county_fullname') if not pd.isna(row.get('county_fullname')) else None
        latitude = row.get('latitude') if not pd.isna(row.get('latitude')) else None
        longitude = row.get('longitude') if not pd.isna(row.get('longitude')) else None
        county_population = row.get('county_population') if not pd.isna(row.get('county_population')) else None
        state_id_sk = row.get('state_id_sk') if not pd.isna(row.get('state_id_sk')) else None

        existing_county = session.query(CountyNDS).filter_by(county_fips=county_fips).first()
        if existing_county:
            existing_county.county_name = county_name
            existing_county.last_updated_nds = pd.Timestamp.utcnow()
        else:
            new_county = CountyNDS(
                county_fips=county_fips,
                county_name=county_name,
                county_fullname=county_fullname,
                latitude=latitude,
                longitude=longitude,
                county_population=county_population,
                state_id_sk=state_id_sk,
                created_date_nds=pd.Timestamp.utcnow(),
                last_updated_nds=pd.Timestamp.utcnow(),
                source_id=1
            )
            session.add(new_county)
        session.commit()
    finally:
        close_session()

#Thêm dữ liệu vào bảng county_nds
def get_merged_county_data():
    session = get_db_session()
    try:
        us_counties = session.query(
            USCountiesStage.county_name,
            USCountiesStage.county_fips,
            USCountiesStage.state_name,
            USCountiesStage.county_fullname,
            USCountiesStage.latitude,
            USCountiesStage.longitude,
            USCountiesStage.county_population
        ).distinct().all()

        state_nds = session.query(
            StateNDS.state_id_sk,
            StateNDS.state_name
        ).distinct().all()

        us_counties_df = pd.DataFrame(us_counties, columns=['county_name', 'county_fips', 'state_name', 'county_fullname', 'latitude', 'longitude', 'county_population'])
        state_nds_df = pd.DataFrame(state_nds, columns=['state_id_sk', 'state_name'])

        merged_df = pd.merge(us_counties_df, state_nds_df, on=['state_name'], how='inner')
        merged_df.apply(lookup_county_id, axis=1)
        data_processing_1()
        data_processing_2()
    finally:
        close_session()

#Thêm vào những dòng dữ liệu có đo lường nhưng không có trong bảng us_counties_stage
def data_processing_1():
    with get_db_session() as session:
        session.execute(
            text("""INSERT INTO county_nds (county_name, created_date_nds, last_updated_nds, state_id_sk, source_id)
                 SELECT DISTINCT s1.county_name, :created_date_nds, :last_updated_nds, s2.state_id_sk, 1
                 FROM state_aqi_stage s1
                 JOIN state_nds s2 ON s1.state_name = s2.state_name
                 WHERE s1.county_name NOT IN (SELECT county_name FROM county_nds);"""),
            {'created_date_nds': datetime.now(), 'last_updated_nds': datetime.now()}
        )
        session.commit()  

def data_processing_2():
    with get_db_session() as session:
        session.execute(
            text("""INSERT INTO county_nds (county_name, created_date_nds, last_updated_nds, state_id_sk, source_id)
                 SELECT DISTINCT s1.county_name, :created_date_nds, :last_updated_nds, s2.state_id_sk, 1
                 FROM state_aqi_stage s1 
                 JOIN state_nds s2 ON s1.state_name = s2.state_name
                 WHERE s1.county_name = 'Windham' AND NOT EXISTS (
                 SELECT 1 FROM us_counties_stage ucs 
                 WHERE ucs.state_name = s1.state_name AND ucs.county_name = s1.county_name
                 );"""),
            {'created_date_nds': datetime.now(), 'last_updated_nds': datetime.now()}
        )
        session.commit()  

#Tìm kiếm dữ liệu đã có trong NDS chưa, nếu có thì cập nhật còn chưa thì thêm mới
def process_measurement_batch(batch, session):
    try:
        to_insert=[]
        for _, row in batch.iterrows():
            existing_measurement = session.query(MeasurementNDS).filter_by(
                measured_date=row['measured_date'],
                defining_site=row['defining_site'],
                defining_parameter=row['defining_parameter']
            ).first()
            if existing_measurement:
                existing_measurement.aqi_value = row['aqi_value']
                existing_measurement.aqi_category = row['aqi_category']
                existing_measurement.last_updated_nds = pd.Timestamp.utcnow()
                existing_measurement.last_updated = pd.Timestamp.utcnow()
            else:
                new_measurement = MeasurementNDS(
                    measured_date=row['measured_date'],
                    aqi_value=row['aqi_value'],
                    aqi_category=row['aqi_category'],
                    defining_parameter=row['defining_parameter'],
                    defining_site=row['defining_site'],
                    num_of_sites_reporting=row['num_of_sites_reporting'],
                    created=row['created'],
                    last_updated=row['last_updated'],
                    created_date_nds=pd.Timestamp.utcnow(),
                    last_updated_nds=pd.Timestamp.utcnow(),
                    county_id_sk=row['county_id_sk'],
                    source_id=1
                )
                to_insert.append(new_measurement)
        if to_insert:
            session.bulk_save_objects(to_insert)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
        
#Thêm dữ liệu vào bảng measurement_nds
def get_merged_measurement_data():
    session = get_db_session()
    try:
        state_nds = session.query(StateNDS.state_id_sk, StateNDS.state_name).distinct().all()
        county_nds = session.query(
            CountyNDS.county_id_sk,
            CountyNDS.state_id_sk,
            CountyNDS.county_name
        ).distinct().all()

        state_aqi = session.query(
            StateAQIStage.county_name,
            StateAQIStage.state_name,
            StateAQIStage.measured_date,
            StateAQIStage.aqi_value,
            StateAQIStage.aqi_category,
            StateAQIStage.defining_parameter,
            StateAQIStage.defining_site,
            StateAQIStage.num_of_sites_reporting,
            StateAQIStage.created,
            StateAQIStage.last_updated
        ).distinct().all()

        state_nds_df = pd.DataFrame(state_nds, columns=['state_id_sk', 'state_name'])
        county_nds_df = pd.DataFrame(county_nds, columns=['county_id_sk', 'state_id_sk', 'county_name'])
        state_aqi_df = pd.DataFrame(state_aqi, columns=[
            'county_name', 'state_name', 'measured_date', 'aqi_value', 'aqi_category',
            'defining_parameter', 'defining_site', 'num_of_sites_reporting', 'created', 'last_updated'
        ])

        merged1_df = pd.merge(state_nds_df, county_nds_df, on=['state_id_sk'], how='inner')
        merged2_df = pd.merge(state_aqi_df, merged1_df, on=['state_name', 'county_name'], how='inner')
        merged2_df = merged2_df.drop_duplicates(subset=['measured_date', 'defining_parameter', 'defining_site'])

        batch_size = 10000
        for start in range(0, len(merged2_df), batch_size):
            batch = merged2_df.iloc[start:start + batch_size]
            process_measurement_batch(batch, session)
    finally:
        close_session()