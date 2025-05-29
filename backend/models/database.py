# backend/models/database.py
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import urlparse

from backend.init import db, app as flask_app # Import app to access its config

def init_db(app):
    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    parsed_uri = urlparse(db_uri)
    db_name = parsed_uri.path.lstrip('/')

    # Construct URI for the default database (e.g., postgres or template1)
    default_db_uri = f"{parsed_uri.scheme}://{parsed_uri.username}:{parsed_uri.password}@{parsed_uri.hostname}:{parsed_uri.port}/postgres"

    engine = create_engine(default_db_uri)
    conn = None  # Initialize conn to None

    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            # Check if database exists
            result = conn.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
            db_exists = result.fetchone()

            if not db_exists:
                conn.execute(f"CREATE DATABASE {db_name}")
                print(f"Database '{db_name}' created successfully.")
            else:
                print(f"Database '{db_name}' already exists.")
    except SQLAlchemyError as e:
        print(f"Error interacting with the database: {e}")
    finally:
        if conn:
            conn.close() # Ensure connection is closed

    # Initialize the database with the app
    db.init_app(app)
    with app.app_context():
        # Import models here to ensure they are registered with SQLAlchemy
        # before create_all() is called
        from backend.models.senha import Senha 
        # Add other models here if you have them
        # from backend.models.unidade import Unidade
        # from backend.models.profissional import Profissional
        db.create_all()
