from flask import Blueprint, jsonify, request, make_response
from ..repositories.mssql.notificaciones import obtener_notificaciones_operador, obtener_notificaciones_coordi
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..models.roles import role_required
from app.extensions import limiter

notificaciones_bp = Blueprint("notificaciones", __name__, url_prefix="/api/notificaciones")

# GET notificaciones del operador
@notificaciones_bp.route("/<int:idOperador>", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def obtener_notificaciones(idOperador):
    currentUser = get_jwt_identity()
    if int(currentUser) != idOperador:
        return make_response(jsonify({'error': 'Acceso no autorizado a las notificaciones de otro operador'}), 403)


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
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def get_notificaciones_coordi():
    limit_str = request.args.get("limit", "")
    
    # Validación de tamaño y tipo
    if limit_str and (not limit_str.isdigit() or len(limit_str) > 5):
        return make_response(jsonify({'error': 'limit inválido'}), 400)
    
    limit = int(limit_str) if limit_str else None
    
    from ..models.notificaciones import NotificacionesCoordi

    validated_params = NotificacionesCoordi(limit = limit)
    # params = validated_params.model_dump(exclude_none=True)

    try:
        result = obtener_notificaciones_coordi(validated_params.limit)
        return make_response(jsonify(result), 200)
        
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)