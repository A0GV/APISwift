from flask import Blueprint, jsonify, request, make_response
from ..repositories.notificaciones import obtener_notificaciones_operador

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