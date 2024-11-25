from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from config.config import DB_CONFIG

# Database connection setup
DATABASE_URI = f"postgresql+psycopg2://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URI)
Base = automap_base()

# Reflect existing tables
Base.prepare(engine, reflect=True)

# Map the tables to ORM classes
StateAQIStage = Base.classes.state_aqi_stage
USCountiesStage = Base.classes.us_counties_stage
Metadata = Base.classes.metadata
StateNDS = Base.classes.state_nds
CountyNDS = Base.classes.county_nds
MeasurementNDS = Base.classes.measurement_nds

# Create a session factory
Session = scoped_session(sessionmaker(bind=engine))
