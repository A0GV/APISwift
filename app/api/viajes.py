from flask import Blueprint, app, jsonify, request, make_response
from ..repositories.mssql.viajes import actualizar_viaje_completo, get_viaje_completo, sql_cancelar_viaje
from flask_jwt_extended import jwt_required
from ..models.roles import role_required
from app.extensions import limiter

viajes_bp = Blueprint("viajes", __name__, url_prefix="/api/viajes")


# GET viaje completo por ID de viaje
@viajes_bp.route("/<int:idViaje>", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def get_viaje(idViaje):
    try:
        viaje = get_viaje_completo(idViaje)
        if viaje:
            return make_response(jsonify(viaje))
        else:
            return make_response(jsonify({'error': 'Viaje no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# PUT actualizar viaje completo
@viajes_bp.route("/<int:idViaje>", methods=['PUT'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def actualizar_viaje(idViaje):
    try:
        data = request.json
        
        # Validar campos requeridos
        required_fields = ['dtFechaInicio', 'dtFechaFin', 'IdAmbulancia', 
                          'IdUsuarioOperador', 'IdNumeroSocio', 'IdTipoTraslado', 
                          'IdUbiOrigen', 'IdUbiDest', 'vcRazon']
        
        for field in required_fields:
            if field not in data:
                return make_response(jsonify({'error': f'Falta el campo requerido: {field}'}), 400)
        
        result = actualizar_viaje_completo(idViaje, data)
        
        if result:
            return make_response(jsonify(result), 200)
        else:
            return make_response(jsonify({'error': 'Viaje no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# PUT cancelar viaje
@viajes_bp.route("/<int:idViaje>/cancelar", methods=['PUT'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def cancelar_viaje(idViaje):
    try:
        result = sql_cancelar_viaje(idViaje)
        
        if result:
            return make_response(jsonify(result), 200)
        else:
            return make_response(jsonify({'error': 'Viaje no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)