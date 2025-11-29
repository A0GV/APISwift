from flask import Blueprint, jsonify, request, make_response
from ..repositories.ambulancias import get_maintenance, get_ambulancias_disponibles, get_tipo_ambulancia_por_id
from ..repositories.operador import sql_read_next_trip

ambulancias_bp = Blueprint("ambulancias", __name__, url_prefix="/api/ambulancias")


@ambulancias_bp.route("/mantenimiento", methods=['GET'])
def dMantenimiento():
    try:
        # Llamar a la función de mysqlfunc para obtener el estado semanal de los traslados
        results = get_maintenance()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


@ambulancias_bp.route("/siguiente-traslado", methods=['GET'])
def next_trip():
    id_operador = request.args.get("idOperador", type=int)
    if not id_operador:
        return make_response(jsonify({"error": "Se requiere idOperador"}), 400)

    try:
        results = sql_read_next_trip(id_operador)
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)
    
    
# GET ambulancias disponibles (filtrando por fecha y hora específica)
@ambulancias_bp.route("/disponibles", methods=['GET'])
def get_ambulancias_disponibles():
    try:
        fecha_inicio = request.args.get('fechaInicio', None)
        fecha_fin = request.args.get('fechaFin', None)
        
        if not fecha_inicio or not fecha_fin:
            return make_response(jsonify({'error': 'Se requieren fechaInicio y fechaFin'}), 400)
        
        # Llamar a la función de mysqlfunc para obtener ambulancias disponibles
        ambulancias = get_ambulancias_disponibles(fecha_inicio, fecha_fin)
        return make_response(jsonify(ambulancias))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET tipo de ambulancia por ID de ambulancia
@ambulancias_bp.route("/<int:idAmbulancia>/tipo", methods=['GET'])
def get_tipo_ambulancia(idAmbulancia):
    try:
        # Llamar a la función de mysqlfunc para obtener el tipo de ambulancia
        tipo = get_tipo_ambulancia_por_id(idAmbulancia)
        
        if tipo:
            return make_response(jsonify(tipo))
        else:
            return make_response(jsonify({'error': 'Ambulancia no encontrada'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
