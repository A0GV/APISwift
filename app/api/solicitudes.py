from flask import Blueprint, make_response, request, jsonify
from ..repositories.solicitudes import get_proximo_numero_solicitud, crear_solicitud_completa

solicitud_bp = Blueprint("solicitudes", __name__, url_prefix="/api/solicitud")

# GET próximo número de solicitud
@solicitud_bp.route("/api/solicitud/proximo-numero", methods=['GET'])
def get_proximo_numero():
    try:
        # Llamar a la función de mysqlfunc para obtener el próximo número
        proximo_numero = get_proximo_numero_solicitud()
        return make_response(jsonify({'proximoNumero': proximo_numero}))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# POST crear solicitud (Traslado + Viaje)
@solicitud_bp.route("/api/solicitud", methods=['POST'])
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


