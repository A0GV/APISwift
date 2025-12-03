import sys
from flask_jwt_extended import JWTManager
from flask import Flask, jsonify
from app.api.login import login_bp
from app.api.traslados import traslados_bp
from app.api.desmadre import main_bp
from app.api.ambulancias import ambulancias_bp
from app.api.notificaciones import notificaciones_bp
from app.api.quejas import quejas_bp
from app.api.operador import operador_bp
from app.api.solicitudes import solicitud_bp
from app.api.viajes import viajes_bp
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from .extensions import limiter

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
    'AWS_REGION': os.getenv('AWS_REGION'),
    'BUCKET': os.getenv('BUCKET')
}

# Connect to mssql dB from start
def create_app():
    app = Flask(__name__)
    jwt = JWTManager(app)
    limiter.init_app(app)

    # Seguridad OWASP: headers CSP y X-Content-Type-Options
    @app.after_request
    def set_security_headers(response):
        response.headers["Content-Security-Policy"] = "default-src 'self'"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers.pop("Server", None)
        return response

    # Seguridad OWASP: manejo de errores genéricos
    @app.errorhandler(Exception)
    def handle_exception(e):
        return jsonify({"error": "Internal server error"}), 500
    limiter = Limiter(get_remote_address, app=app, default_limits=["1000 per hour"])
    app.config['JWT_ALGORITHM'] = 'HS256'
    app.config['JWT_SECRET_KEY']= os.getenv('JWT_SECRET_KEY')
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = int(os.getenv('JWT_ACCESS_TOKEN_EXPIRES'))  # 3600 segundos (1 hora)
    app.config['JWT_TOKEN_LOCATION'] = ['headers']
    app.config['JWT_HEADER_NAME'] = 'Authorization'
    app.config['JWT_HEADER_TYPE'] = 'Bearer'
   

    try:
        db.init_app(mssql_params) # Esto ya crea la conexión en el modulo de db y la configura con las credenciales.
    except Exception as e:
        print("Cannot connect to mssql server!: {}".format(e))
        sys.exit()
    
    try:
        s3.init_app(s3_params)
    except Exception as e:
        print("Cannot initialize S3 server configuration!: {}".format(e))
        sys.exit()
        
    # En app/__init__.py, dentro de create_app(), después de los otros handlers:

    @app.route('/debug-token', methods=['POST'])
    def debug_token():
        from flask import request
        import jwt
        
        data = request.json
        token = data.get('token')
        
        if not token:
            return jsonify({'error': 'No token provided'}), 400
        
        try:
            # Decodificar sin verificar para ver el contenido
            decoded = jwt.decode(
                token, 
                options={"verify_signature": False}
            )
            return jsonify({
                'decoded_token': decoded,
                'has_sub': 'sub' in decoded,
                'identity_claim': app.config.get('JWT_IDENTITY_CLAIM', 'sub')
            }), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    app.register_blueprint(login_bp)
    app.register_blueprint(traslados_bp)
    app.register_blueprint(ambulancias_bp)
    app.register_blueprint(notificaciones_bp)
    app.register_blueprint(operador_bp)
    app.register_blueprint(quejas_bp)
    app.register_blueprint(solicitud_bp)
    app.register_blueprint(viajes_bp)
    app.register_blueprint(main_bp)

    return app
