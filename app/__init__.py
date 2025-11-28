from flask import Flask
from app.api.login import login_bp
from app.api.traslados import traslados_bp
from app.api.desmadre import main_bp
from app.api.ambulancias import ambulancias_bp
from app.api.notificaciones import notificaciones_bp

import sys
from app.extensions import db  # Esta es una instancia de la clase, todavía no está configurada

mssql_params = {}
mssql_params['DB_HOST'] = '100.80.80.7'
mssql_params['DB_NAME'] = 'nova'
mssql_params['DB_USER'] = 'SA'
mssql_params['DB_PASSWORD'] = 'Shakira123.' 

# Connect to mssql dB from start
def create_app():
    app = Flask(__name__)
    app.register_blueprint(login_bp)
    app.register_blueprint(traslados_bp)
    app.register_blueprint(ambulancias_bp)
    app.register_blueprint(notificaciones_bp)
    app.register_blueprint(main_bp)

    try:
        db.init_app(mssql_params) # Esto ya crea la conexión en el modulo de db y la configura con las credenciales.
    except Exception as e:
        print("Cannot connect to mssql server!: {}".format(e))
        sys.exit()
    return app
