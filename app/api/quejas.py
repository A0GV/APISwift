
from flask import Blueprint, jsonify, request, make_response
from ..repositories.quejas import getQuejas

quejas_bp = Blueprint("quejas", __name__, url_prefix="/api/quejas")

# GET quejas activas
@quejas_bp.route("/obtenerquejas", methods=["GET"])
def obtener_quejas():
	try:
		results = getQuejas()
		return make_response(jsonify(results), 200)
	except Exception as e:
		return make_response(jsonify({'error': str(e)}), 500)

