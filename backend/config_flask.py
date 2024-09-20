# backend/config_flask.py

class Config:
    SECRET_KEY = 'supersecretkey123!@#'
    SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:esus@localhost:5432/esus'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
