# backend/init.py
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config_flask import Config  # Importe a configuração do banco de dados de config_flask.py
from flask_cors import CORS 

# Inicialize o aplicativo Flask
app = Flask(__name__)

# Aplique a configuração do banco de dados a partir do arquivo de configuração
app.config.from_object(Config)

# Inicialize o SQLAlchemy
db = SQLAlchemy(app)
cors_allowed_origins="*"