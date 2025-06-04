#backend/Conexoes.py
from sqlalchemy import create_engine, text # Added text
import json
import os
from datetime import datetime
import logging

# Logger for this module
logger = logging.getLogger(__name__)
# Basic configuration for the logger if not configured elsewhere
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def get_local_engine():
    local_db_uri = "postgresql+psycopg2://postgres:esus@localhost:5432/esus"
    logger.info(f"Attempting to connect to local database: {local_db_uri.replace('postgres:esus', 'postgres:*****')}")
    return create_engine(
        local_db_uri,
        connect_args={'options': '-c client_encoding=utf8'},
        client_encoding='utf8'
    )

def get_external_engine():
    config = load_config()
    required_keys = ['username', 'password', 'ip', 'port', 'database']
    if not config or not all(key in config for key in required_keys):
        logger.error("External database configuration is missing or incomplete in config.json.")
        # Depending on desired behavior, could raise an error or return None
        raise ValueError("External database configuration is missing or incomplete.")

    external_db_uri = (
        f"postgresql+psycopg2://{config['username']}:{config['password']}"
        f"@{config['ip']}:{config['port']}/{config['database']}"
    )
    safe_uri = external_db_uri.replace(config['password'], '*****')
    logger.info(f"Attempting to connect to external database: {safe_uri}")

    return create_engine(
        external_db_uri,
        connect_args={'client_encoding': 'utf8'}
    )

def load_config(config_path='config.json'): # Made config_path a parameter for flexibility
    if not os.path.exists(config_path):
        logger.error(f"Configuration file '{config_path}' not found.")
        # Return a default/empty config or raise error, depends on desired handling
        # For now, let's raise an error as other functions depend on it.
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from file '{config_path}': {e}")
        raise ValueError(f"Error decoding JSON from file '{config_path}': {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred loading config '{config_path}': {e}")
        raise
    return config_data

# Re-evaluating log_message. The main app likely has its own logger.
# For now, this function will log to a local file and print.
# Ideally, this module should use the Flask app's logger if integrated.
def log_message(message, log_file_name="connections_log.txt"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"{timestamp} - {message}"

    log_dir = "logs" # Centralized logs directory
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            # Fallback if directory creation fails (e.g. permissions)
            print(f"Error creating log directory '{log_dir}': {e}. Logging to current directory.")
            log_file_path = log_file_name
    else:
        log_file_path = os.path.join(log_dir, log_file_name)

    try:
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(full_message + "\n")
    except Exception as e:
        print(f"Failed to write to log file {log_file_path}: {e}")

    print(full_message) # Also print to console for immediate visibility if needed

def test_db_connection(config_data: dict) -> tuple[bool, str | None]:
    """
    Tests a database connection using the provided configuration data.

    Args:
        config_data: A dictionary with 'username', 'password', 'ip', 'port', 'database'.
                     (Note: 'ip' is used in the original prompt, but 'host' is more standard for SQLAlchemy)
                     Using 'ip' as per prompt for now.

    Returns:
        A tuple (is_successful: bool, error_message: str | None).
    """
    # Use 'host' if 'ip' is not present, for broader compatibility
    host_or_ip = config_data.get('ip') or config_data.get('host')

    required_keys_display = ['username', 'password', 'ip (or host)', 'port', 'database']
    # Actual keys to check for existence
    required_keys_internal = ['username', 'password', 'port', 'database']

    missing_keys = [key_display for key_internal, key_display in zip(required_keys_internal, required_keys_display) if not config_data.get(key_internal)]
    if not host_or_ip:
        missing_keys.append('ip (or host)')

    if missing_keys:
        error_msg = f"Missing required configuration keys: {', '.join(missing_keys)}"
        logger.warning(f"Connection test failed due to missing keys: {', '.join(missing_keys)}")
        return False, error_msg

    try:
        # Construct connection string carefully, ensuring values are not None
        user = config_data['username']
        password = config_data['password']
        db_host = host_or_ip
        port = config_data['port']
        dbname = config_data['database']

        conn_str = f"postgresql://{user}:{password}@{db_host}:{port}/{dbname}"

        engine = create_engine(
            conn_str,
            connect_args={'options': '-c client_encoding=utf8', 'connect_timeout': 5},
            pool_pre_ping=True
        )
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        logger.info(f"Successfully connected to database {dbname} at {db_host}.")
        return True, None
    except Exception as e:
        error_message_detail = f"Database connection failed for {config_data.get('database')}@{host_or_ip}: {str(e)}"
        logger.error(error_message_detail)
        return False, str(e) # Return the raw error for the route to format

# Example of how the main app's logger could be passed and used (optional refactor)
# def set_app_logger(app_logger):
# global logger
# logger = app_logger
# logger.info("Conexoes module is now using the main application logger.")
