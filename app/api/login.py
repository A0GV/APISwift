from flask import Blueprint, jsonify, request, make_response
from ..repositories.mssql.mysqlfunc import sql_read_where, sql_read_where
from flask_jwt_extended import create_access_token
import hashlib


login_bp = Blueprint("login", __name__, url_prefix="/api/login")

def verificar_password(password_plana, password_hash, salt):
    """
    Verifica si la contraseña en texto plano coincide con el hash almacenado.
    
    Args:
        password_plana: La contraseña ingresada por el usuario
        password_hash: El hash almacenado en la base de datos
        salt: El salt asociado a la contraseña
    
    Returns:
        True si la contraseña es correcta, False en caso contrario
    """
    # Concatenar el salt con la contraseña
    password_con_salt = salt + password_plana
    
    # Generar el hash usando SHA-256 (ajusta según tu algoritmo)
    hash_generado = hashlib.sha256(password_con_salt.encode('utf-8')).hexdigest()
    
    # Comparar el hash generado con el almacenado
    return hash_generado == password_hash


# ========== ENDPOINTS DE LOGIN ==========
@login_bp.route("/operador", methods=['POST'])
def login_operador_post():
    data = request.json
    id_usuario = data.get('idUsuario', None)
    vc_codigo = data.get('codigo', None)
    
    try:
        # Primero buscar el usuario
        usuario = sql_read_where('Usuarios', {'IdUsuario': id_usuario, 'IdTipoPersonal': 1})
        if not usuario:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
        
        # Obtener el IdPass del usuario
        id_pass = usuario[0]['IdPass']
        
        # Obtener el registro de contraseña con el salt
        password_record = sql_read_where('Pass', {'IdPass': id_pass})
        
        if not password_record:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
        
        # Extraer los datos necesarios
        password_hash = password_record[0]['vcCodigoUsuario']  # El hash almacenado
        salt = password_record[0]['Salt']  # El salt almacenado
        
        # Verificar la contraseña
        if verificar_password(vc_codigo, password_hash, salt):
            access_token = create_access_token(
                identity=str(id_usuario),
                additional_claims={'rol': 'operador'}
            )            
            return make_response(jsonify({'access_token': access_token, 'IdUsuario': id_usuario}), 200)
        else:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)

    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# Login para COORDINADORES (IdTipoPersonal = 2)
@login_bp.route("/coordinador", methods=['POST'])
def login_coordinador():
    data = request.json
    id_usuario = data.get('idUsuario', None)
    vc_codigo = data.get('codigo', None)
    
    if not id_usuario or not vc_codigo:
        return make_response(jsonify({'error': 'Se requieren idUsuario y vcCodigoUsuario'}), 400)
    
    try:
        # Primero buscar el usuario
        usuario = sql_read_where('Usuarios', {'IdUsuario': id_usuario, 'IdTipoPersonal': 2})
        
        if not usuario:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
        
        # Obtener el IdPass del usuario
        id_pass = usuario[0]['IdPass']
        
        # Obtener el registro de contraseña con el salt
        password_record = sql_read_where('Pass', {'IdPass': id_pass})
        
        if not password_record:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
        
        # Extraer los datos necesarios
        password_hash = password_record[0]['vcCodigoUsuario']  # El hash almacenado
        salt = password_record[0]['Salt']  # El salt almacenado
        
        # Verificar la contraseña
        if verificar_password(vc_codigo, password_hash, salt):
            access_token = create_access_token(
                identity=str(id_usuario),
                additional_claims={'rol': 'coordinador'}
            )
            return make_response(jsonify({'access_token': access_token, 'IdUsuario': id_usuario}), 200)
        else:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
            
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
