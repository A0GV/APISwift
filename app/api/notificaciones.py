from flask import Blueprint, jsonify, request, make_response
from ..repositories.notificaciones import obtener_notificaciones_operador, obtener_notificaciones_coordi

notificaciones_bp = Blueprint("notificaciones", __name__, url_prefix="/api/notificaciones")

# GET notificaciones del operador
@notificaciones_bp.route("/<int:idOperador>", methods=['GET'])
def obtener_notificaciones(idOperador):
    
    limite = request.args.get("limite", None)
    from ..models.notificaciones import NotificacionesOperador
    validated_params = NotificacionesOperador(idOperador = idOperador, limite = limite)
    # params = validated_params.model_dump(exclude_none=True)

    try:
        result = obtener_notificaciones_operador(idOperador, validated_params.limite)
        
        return make_response(jsonify(result), 200)
        
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# GET notificaciones del coordinador
# https://upon-quarters-feof-handbook.trycloudflare.com/api/notificaciones/coordi?limit=10
@notificaciones_bp.route("/coordi", methods=['GET'])
def get_notificaciones_coordi():
    limit = request.args.get("limit", type=int)
    
    from ..models.notificaciones import NotificacionesCoordi

    validated_params = NotificacionesCoordi(limit = limit)
    # params = validated_params.model_dump(exclude_none=True)

    try:
        result = obtener_notificaciones_coordi(validated_params.limit)
        return make_response(jsonify(result), 200)
        
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)