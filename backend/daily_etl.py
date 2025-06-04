#!/usr/bin/env python3
# /home/ubuntu/esus_project/scripts/etl/daily_etl.py

import sys
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging
import traceback
from datetime import datetime
import unicodedata

# Adjust path to import modules from the backend directory
backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))
sys.path.insert(0, backend_path)

# Import necessary functions from backend modules
# Note: Adjust imports if function names or locations differ slightly
try:
    # Assuming Conexoes.py and banco.py are in the backend_path
    from Conexoes import load_config as backend_load_config, get_local_engine as backend_get_local_engine # Changed from Conexões
    from banco import clean_dataframe, rename_duplicate_columns # Use cleaning functions from banco.py
    # Import query definitions if they are centralized, otherwise define here
    # from Consultas import get_queries_for_type # Example if queries are in Consultas.py
except ImportError as e:
    print(f"Error importing backend modules: {e}")
    print(f"Ensure backend modules are accessible from: {backend_path}")
    sys.exit(1)

# --- Configuration ---
LOG_FILE = os.path.join(os.path.dirname(__file__), 'etl_log.txt')
CONFIG_FILE = os.path.join(backend_path, 'config.json')

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout) # Also print logs to console
    ]
)

# --- Database Connection Functions ---
def get_local_engine():
    """Gets the SQLAlchemy engine for the local database."""
    try:
        engine = backend_get_local_engine()
        logging.info("Successfully connected to local database.")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to local database: {e}")
        raise

def get_external_engine():
    """Gets the SQLAlchemy engine for the external database using config.json."""
    try:
        config = backend_load_config() # Use function from Conexoes.py
        # Use placeholders if config is not fully set, log warning
        db_ip = config.get('ip', 'YOUR_EXTERNAL_DB_IP')
        db_port = config.get('port', '5432')
        db_name = config.get('database', 'YOUR_EXTERNAL_DB_NAME')
        db_user = config.get('username', 'YOUR_EXTERNAL_DB_USER')
        db_pass = config.get('password', 'YOUR_EXTERNAL_DB_PASSWORD')

        if 'YOUR_EXTERNAL_DB' in db_ip or 'YOUR_EXTERNAL_DB' in db_name or 'YOUR_EXTERNAL_DB' in db_user:
             logging.warning("External database configuration seems incomplete. Using placeholders.")
             # As external connection will fail in sandbox, fallback to local for testing structure
             logging.info("Falling back to local DB connection for ETL structure test.")
             return get_local_engine()

        external_db_uri = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_ip}:{db_port}/{db_name}"
        engine = create_engine(
            external_db_uri,
            connect_args={'options': '-c client_encoding=utf8'}
        )
        # Test connection
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        logging.info(f"Successfully connected to external database: {db_ip}:{db_port}/{db_name}")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to external database using config from {CONFIG_FILE}: {e}")
        logging.warning("ETL will proceed using the local database connection if possible.")
        # Fallback to local engine if external fails or is not configured
        return get_local_engine()

@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        logging.error("Session rollback due to error.")
        raise
    finally:
        session.close()

# --- Query Definitions (Placeholder - Adapt from Consultas.py) ---
def get_queries_for_etl(data_type):
    """Returns the SQL query and target table name for a given data type."""
    # IMPORTANT: Adapt these queries from Consultas.py or the source
    # These are simplified placeholders
    queries = {
        'cadastro': {
            'query': "SELECT co_seq_cidadao as id, no_cidadao as nome, dt_nascimento FROM tb_cidadao WHERE st_ativo = 1 LIMIT 100", # Example query
            'table': 'tb_cadastro_local'
        },
        'domiciliofcd': {
            'query': "SELECT co_seq_cds_cad_domiciliar as id, no_logradouro, nu_domicilio FROM tb_cds_cad_domiciliar LIMIT 100", # Example query
            'table': 'tb_domicilio_local'
        },
        # Add other types: 'bpa', 'visitas', 'iaf', 'pse', 'pse_prof'
        # ...
    }
    return queries.get(data_type)

# --- ETL Process Function ---
def run_etl_for_type(data_type, external_engine, local_engine):
    """Runs the ETL process for a specific data type."""
    logging.info(f"Starting ETL for data type: {data_type}")

    query_info = get_queries_for_etl(data_type)
    if not query_info:
        logging.warning(f"No query definition found for data type: {data_type}. Skipping.")
        return

    query = query_info['query']
    target_table = query_info['table']
    chunksize = 10000  # Process data in chunks

    try:
        rows_processed = 0
        first_chunk = True
        with external_engine.connect() as ext_conn, local_engine.connect() as loc_conn:
            # Use stream_results=True for potentially large datasets
            # ext_conn = ext_conn.execution_options(stream_results=True)
            result_proxy = ext_conn.execute(text(query))

            while True:
                logging.info(f"Fetching chunk for {data_type}...")
                chunk = result_proxy.fetchmany(chunksize)
                if not chunk:
                    logging.info(f"No more data for {data_type}.")
                    break

                df = pd.DataFrame(chunk, columns=result_proxy.keys())
                logging.info(f"Fetched {len(df)} rows for {data_type}.")

                # Clean the dataframe
                df = clean_dataframe(df) # Use function from banco.py

                # Write chunk to local database
                write_mode = 'replace' if first_chunk else 'append'
                try:
                    with session_scope(local_engine) as session:
                         df.to_sql(target_table, con=session.bind, if_exists=write_mode, index=False, chunksize=1000) # Use chunksize for writing too
                    logging.info(f"Successfully wrote {len(df)} rows to local table {target_table} (mode: {write_mode}).")
                    rows_processed += len(df)
                    first_chunk = False
                except Exception as write_error:
                    logging.error(f"Error writing chunk to local table {target_table}: {write_error}")
                    # Optionally break or continue based on error handling strategy
                    raise write_error # Re-raise to stop ETL for this type on error

        logging.info(f"ETL completed for {data_type}. Total rows processed: {rows_processed}")

    except Exception as e:
        error_details = traceback.format_exc()
        logging.error(f"ETL failed for data type {data_type}: {e}\nDetails: {error_details}")

# --- Main Execution ---
def main():
    logging.info("=== Starting Daily ETL Process ===")
    start_time = datetime.now()

    try:
        local_engine = get_local_engine()
        external_engine = get_external_engine()
    except Exception as conn_err:
        logging.error(f"Database connection failed, cannot proceed with ETL: {conn_err}")
        sys.exit(1)

    # List of data types to process
    data_types_to_process = [
        'cadastro',
        'domiciliofcd',
        # 'bpa', # Add other types as needed
        # 'visitas',
        # 'iaf',
        # 'pse',
        # 'pse_prof'
    ]

    for data_type in data_types_to_process:
        run_etl_for_type(data_type, external_engine, local_engine)

    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"=== Daily ETL Process Finished ===")
    logging.info(f"Total duration: {duration}")

if __name__ == "__main__":
    main()

