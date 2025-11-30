from flask_jwt_extended import get_jwt
from functools import wraps
from flask import jsonify

def role_required(required_roles):
    # Convertir string a lista si es necesario
    if isinstance(required_roles, str):
        required_roles = [required_roles]
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            claims = get_jwt()
            user_role = claims.get('rol')

            if user_role not in required_roles:
                return jsonify({'error': 'No tienes permiso para acceder a este recurso'}), 403

            return func(*args, **kwargs)
        return wrapper
    return decorator