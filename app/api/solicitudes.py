from flask import Blueprint, make_response, request, jsonify
from ..repositories.mssql.solicitudes import get_proximo_numero_solicitud, crear_solicitud_completa, get_catalogos
from ..repositories.mssql.mysqlfunc import sql_read_where
from flask_jwt_extended import jwt_required
from ..models.roles import role_requir

solicitud_bp = Blueprint("solicitudes", __name__, url_prefix="/api/solicitud")

# GET próximo número de solicitud
@solicitud_bp.route("/proximo-numero", methods=['GET'])
def get_proximo_numero():
    try:
        # Llamar a la función de mysqlfunc para obtener el próximo número
        proximo_numero = get_proximo_numero_solicitud()
        return make_response(jsonify({'proximoNumero': proximo_numero}))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# POST crear solicitud (Traslado + Viaje)
@solicitud_bp.route("/", methods=['POST'])
def crear_solicitud():
    try:
        data = request.json
        
        # Validar datos requeridos
        required_fields = ['IdUsuarioCoord', 'IdUsuarioOperador', 'IdNumeroSocio', 
                          'IdTipoTraslado', 'IdUbiOrigen', 'IdUbiDest', 'vcRazon',
                          'dtFechaInicio', 'dtFechaFin', 'IdAmbulancia']
        
        for field in required_fields:
            if field not in data:
                return make_response(jsonify({'error': f'Falta el campo requerido: {field}'}), 400)
        
        # Llamar a la función de mysqlfunc para crear la solicitud
        result = crear_solicitud_completa(data)
        
        return make_response(jsonify(result), 201)
        
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
    



@solicitud_bp.route("/catalogos", methods=['GET'])
def get_catalogos_sol():
    try:
        catalogos = get_catalogos()
        return make_response(jsonify(catalogos))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
    
@solicitud_bp.route("/socios/<int:numeroSocio>", methods=['GET'])
def get_socio(numeroSocio):
    try:
        socio = sql_read_where('Socios', {'IdNumeroSocio': numeroSocio})
        if socio:
            return make_response(jsonify(socio[0]))
        else:
            return make_response(jsonify({'error': 'Socio no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)