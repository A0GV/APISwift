from flask import Blueprint, jsonify, request, make_response

from app.repositories.mssql.mysqlfunc import sql_read_all
from ..repositories.mssql.quejas import get_proximo_numero_queja, getQuejas, updateQuejaEstado
from ..extensions import db
from flask_jwt_extended import jwt_required
from ..models.roles import role_required
import pymssql

quejas_bp = Blueprint("quejas", __name__, url_prefix="/api/quejas")

# GET quejas activas
@quejas_bp.route("/obtenerquejas", methods=["GET"])
def obtener_quejas():
	try:
		results = getQuejas()
		return make_response(jsonify(results), 200)
	except Exception as e:
		return make_response(jsonify({'error': str(e)}), 500)

# PUT endpoint to update the status of a complaint
@quejas_bp.route("/actualizarEstado", methods=["PUT"])
def actualizar_estado():
    try:
        data = request.get_json()
        id_queja = data.get("IdQueja")

        if not id_queja:
            return make_response(jsonify({"error": "El campo 'IdQueja' es obligatorio."}), 400)

        updateQuejaEstado(id_queja)

        return make_response(jsonify({"message": "Estado actualizado correctamente."}), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


# GET catálogos para quejas (ambulancias + próximo número)
@quejas_bp.route("/catalogos-queja", methods=['GET'])
def get_catalogos_queja():
    try:
        ambulancias = sql_read_all('Ambulancia')
        proximo_numero = get_proximo_numero_queja()
        
        return make_response(jsonify({
            'ambulancias': ambulancias,
            'proximoNumero': proximo_numero
        }))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
