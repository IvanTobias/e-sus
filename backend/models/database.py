# backend/models/database.py
from init import db  # Importe a instância do banco de dados de init.py

def init_db(app):
    # Inicialize o banco de dados
    db.init_app(app)
    with app.app_context():
        db.create_all()
