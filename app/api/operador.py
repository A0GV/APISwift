from flask import Blueprint, jsonify, request, make_response
from ..repositories.operador import get_user_data

operador_bp = Blueprint("operadores", __name__, url_prefix = "/api/operadores")

@operador_bp.route("/<int:idOperador>/datos", methods=['GET'])
def get_datos_operador(idOperador):
    try:
        operador_data = get_user_data(idOperador)   
        if operador_data:
            return make_response(jsonify(operador_data), 200)
        else:
            return make_response(jsonify({'error': 'Operador no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
