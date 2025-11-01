# BTS Flight Data ETL Script - Customized for 2021-2022 Data
# Loads Bureau of Transportation Statistics flight data into PostgreSQL
# Author: Patrick Mead
# Last Updated: 2025


import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# database configuration
DB_CONFIG = {
    'dbname': 'bts_flights',
    'user': 'postgres',
    'password': 'Missoula4061!',  # UPDATE THIS
    'host': 'localhost',
    'port': '5432'
}

# column mapping
COLUMN_MAPPING = {
    # Date/Time
    'FL_DATE': 'flight_date',
    'YEAR': 'year',
    'QUARTER': 'quarter',
    'MONTH': 'month',
    'DAY_OF_MONTH': 'day_of_month',
    'DAY_OF_WEEK': 'day_of_week',
    
    # Flight/Carrier Identifiers
    'OP_UNIQUE_CARRIER': 'carrier_code',
    'OP_CARRIER': 'carrier_code_alt',
    'OP_CARRIER_AIRLINE_ID': 'carrier_airline_id',
    'TAIL_NUM': 'tail_number',
    'OP_CARRIER_FL_NUM': 'flight_number',
    
    # Origin Airport
    'ORIGIN': 'origin_airport',
    'ORIGIN_AIRPORT_ID': 'origin_airport_id',
    'ORIGIN_CITY_NAME': 'origin_city_name',
    'ORIGIN_STATE_ABR': 'origin_state',
    'ORIGIN_STATE_NM': 'origin_state_name',
    
    # Destination Airport
    'DEST': 'dest_airport',
    'DEST_AIRPORT_ID': 'dest_airport_id',
    'DEST_CITY_NAME': 'dest_city_name',
    'DEST_STATE_ABR': 'dest_state',
    'DEST_STATE_NM': 'dest_state_name',
    
    # Scheduled Times
    'CRS_DEP_TIME': 'crs_dep_time',
    'CRS_ARR_TIME': 'crs_arr_time',
    
    # Actual Times
    'DEP_TIME': 'dep_time',
    'ARR_TIME': 'arr_time',
    'WHEELS_OFF': 'wheels_off',
    'WHEELS_ON': 'wheels_on',
    
    # Departure Performance
    'DEP_DELAY': 'dep_delay',
    'DEP_DELAY_NEW': 'dep_delay_minutes',
    'DEP_DEL15': 'dep_del15',
    'DEP_DELAY_GROUP': 'dep_delay_group',
    
    # Arrival Performance
    'ARR_DELAY': 'arr_delay',
    'ARR_DELAY_NEW': 'arr_delay_minutes',
    'ARR_DEL15': 'arr_del15',
    'ARR_DELAY_GROUP': 'arr_delay_group',
    
    # Taxi and Ground Times
    'TAXI_OUT': 'taxi_out',
    'TAXI_IN': 'taxi_in',
    
    # Flight Duration
    'ACTUAL_ELAPSED_TIME': 'actual_elapsed_time',
    'CRS_ELAPSED_TIME': 'crs_elapsed_time',
    'AIR_TIME': 'air_time',
    
    # Distance
    'DISTANCE': 'distance',
    'DISTANCE_GROUP': 'distance_group',
    
    # Cancellations & Diversions
    'CANCELLED': 'cancelled',
    'CANCELLATION_CODE': 'cancellation_code',
    'DIVERTED': 'diverted',
    
    # Delay Causes
    'CARRIER_DELAY': 'carrier_delay',
    'WEATHER_DELAY': 'weather_delay',
    'NAS_DELAY': 'nas_delay',
    'SECURITY_DELAY': 'security_delay',
    'LATE_AIRCRAFT_DELAY': 'late_aircraft_delay',
    
    # Additional Fields
    'FLIGHTS': 'flights_count'
}

# helper functions
# create db connection
def create_database_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
# create sqlalchemy connection
def create_sqlalchemy_engine():
    conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(conn_string)

# load airports
def load_airports_from_first_file(csv_file, conn):
    logger.info(f"Extracting airports from {os.path.basename(csv_file)}...")
    
    df = pd.read_csv(csv_file, usecols=['ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR',
                                          'DEST', 'DEST_CITY_NAME', 'DEST_STATE_ABR'])
    cursor = conn.cursor()
    
    # Get unique origin airports
    origin_airports = df[['ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR']].drop_duplicates()
    dest_airports = df[['DEST', 'DEST_CITY_NAME', 'DEST_STATE_ABR']].drop_duplicates()
    
    # Rename for consistency
    origin_airports.columns = ['airport_code', 'city_name', 'state_code']
    dest_airports.columns = ['airport_code', 'city_name', 'state_code']
    
    # Combine and deduplicate
    all_airports = pd.concat([origin_airports, dest_airports]).drop_duplicates('airport_code')
    
    for _, row in all_airports.iterrows():
        cursor.execute("""
            INSERT INTO airports (airport_code, city_name, state_code) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (airport_code) DO NOTHING
        """, (row['airport_code'], row['city_name'], row['state_code']))
    
    conn.commit()
    logger.info(f"Loaded {len(all_airports)} airports")
    cursor.close()

# load airlines
def load_airlines_from_first_file(csv_file, conn):
    logger.info(f"Extracting airlines from {os.path.basename(csv_file)}...")
    
    df = pd.read_csv(csv_file, usecols=['OP_UNIQUE_CARRIER'])
    unique_carriers = df['OP_UNIQUE_CARRIER'].dropna().unique()
    
    cursor = conn.cursor()
    
    for carrier_code in unique_carriers:
        cursor.execute("""
            INSERT INTO airlines (carrier_code) 
            VALUES (%s) 
            ON CONFLICT (carrier_code) DO NOTHING
        """, (carrier_code,))
    
    conn.commit()
    logger.info(f"Loaded {len(unique_carriers)} airlines")
    cursor.close()

# clean flight data
def clean_flight_data(df):
    logger.info(f"Cleaning data: {len(df)} rows...")
    
    # Rename columns to match database schema
    df_clean = df.rename(columns=COLUMN_MAPPING)
    
    # Keep only columns that exist in our mapping
    mapped_columns = list(COLUMN_MAPPING.values())
    df_clean = df_clean[[col for col in mapped_columns if col in df_clean.columns]]
    
    # Convert flight_date to proper date format
    if 'flight_date' in df_clean.columns:
        df_clean['flight_date'] = pd.to_datetime(df_clean['flight_date'], errors='coerce')
    
    # Fill NaN values for binary fields
    binary_cols = ['cancelled', 'diverted', 'dep_del15', 'arr_del15']
    for col in binary_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna(0).astype('Int64')
    
    # Fill NaN for delay fields with 0
    delay_cols = ['dep_delay', 'arr_delay', 'dep_delay_minutes', 'arr_delay_minutes',
                  'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay']
    for col in delay_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna(0)
    
    logger.info(f"Cleaned: {len(df_clean)} rows, {len(df_clean.columns)} columns")
    return df_clean

# load flight data
def load_flights_data(csv_file, engine, chunk_size=50000):
    logger.info(f"Loading flight data from {os.path.basename(csv_file)}...")
    
    total_rows = 0
    chunk_number = 0
    
    try:
        # Read and process CSV in chunks
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False):
            chunk_number += 1
            logger.info(f"  Chunk {chunk_number}: {len(chunk)} rows...")
            
            # Clean the chunk
            chunk_clean = clean_flight_data(chunk)
            
            # Load to database
            chunk_clean.to_sql('flights', engine, if_exists='append', index=False)
            total_rows += len(chunk)
            
        return total_rows
        
    except Exception as e:
        logger.error(f"Error loading {os.path.basename(csv_file)}: {e}")
        raise

# find csvs in the base folder
def find_all_csv_files(base_folder):
    csv_files = []
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return sorted(csv_files)

# etl process
def load_multiple_files(csv_files):
    start_time = datetime.now()
    logger.info("BTS FLIGHT DATA ETL PROCESS")
    
    try:
        # Create connections
        conn = create_database_connection()
        engine = create_sqlalchemy_engine()
        
        # Load reference data from first file
        if csv_files:
            load_airlines_from_first_file(csv_files[0], conn)
            load_airports_from_first_file(csv_files[0], conn)
        
        # Load each file
        total_rows = 0
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"File {i}/{len(csv_files)}: {os.path.basename(csv_file)}")
            rows = load_flights_data(csv_file, engine)
            total_rows += rows
        
        # Close connections
        conn.close()
        
        # Calculate runtime
        end_time = datetime.now()
        runtime = end_time - start_time
        logger.info("ETL COMPLETED SUCCESSFULLY!")
        logger.info(f"Total rows loaded: {total_rows:,}")
        logger.info(f"Files processed: {len(csv_files)}")
        logger.info(f"Runtime: {runtime}")
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    # enter base folder
    BASE_FOLDER = "BASE FOLDER"
    
    if not os.path.exists(BASE_FOLDER):
        logger.error(f"Folder not found: {BASE_FOLDER}")
    else:
        csv_files = find_all_csv_files(BASE_FOLDER)
        
        if not csv_files:
            logger.error(f"No CSV files found in {BASE_FOLDER}")
            logger.error("Please download BTS data and save as YYYY_MM.csv")
        else:
            logger.info(f"\nFound {len(csv_files)} CSV files:")
            response = input(f"\nLoad {len(csv_files)} files into database? (yes/no): ")
            if response.lower() in ['yes', 'y']:
                load_multiple_files(csv_files)
            else:
                logger.info("Load cancelled by user")
