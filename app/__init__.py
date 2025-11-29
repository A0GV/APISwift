import sys

from flask import Flask
from app.api.login import login_bp
from app.api.traslados import traslados_bp
from app.api.desmadre import main_bp
from app.api.ambulancias import ambulancias_bp
from app.api.notificaciones import notificaciones_bp
from app.api.operador import operador_bp

from app.extensions import db, s3

from dotenv import load_dotenv
import os

load_dotenv()

mssql_params = {
    'DB_HOST': os.getenv('DB_HOST'),
    'DB_NAME': os.getenv('DB_NAME'),
    'DB_USER': os.getenv('DB_USER'),
    'DB_PASSWORD': os.getenv('DB_PASSWORD')
}

s3_params = {
    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'AWS_REGION': os.getenv('AWS_REGION')
}

# Connect to mssql dB from start
def create_app():
    app = Flask(__name__)

    try:
        db.init_app(mssql_params) # Esto ya crea la conexión en el modulo de db y la configura con las credenciales.
    except Exception as e:
        print("Cannot connect to mssql server!: {}".format(e))
        sys.exit()
    
    try:
        s3.init_app(s3_params)
    except Exception as e:
        print("Cannot connect to S3 server!: {}".format(e))
        sys.exit()

    
    app.register_blueprint(login_bp)
    app.register_blueprint(traslados_bp)
    app.register_blueprint(ambulancias_bp)
    app.register_blueprint(notificaciones_bp)
    app.register_blueprint(operador_bp)
    app.register_blueprint(main_bp)

    return app
