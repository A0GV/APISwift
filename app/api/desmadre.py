from flask import jsonify, make_response, request, Blueprint
from ..repositories.estadisticas import get_demand_hours, get_operators_with_most_transfers, get_monthly_transfer_percentages, get_weekly_transfer_status
from ..repositories.ambulancias import sql_read_last_amt_km
from ..repositories.traslados import sql_update_start_trip, sql_update_end_trip
from ..repositories.mysqlfunc import sql_read_all, sql_read_where, sql_update_where

main_bp = Blueprint("main", __name__)


@main_bp.route("/hello")
def hello():
    return "Shakira rocks!\n"

@main_bp.route("/horas/demanda", methods=['GET'])
def demand_hours():
    try:
        # Llamar a la función de mysqlfunc para obtener los horarios con mayor demanda
        results = get_demand_hours()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

@main_bp.route("/operador/mas-traslados", methods=['GET'])
def operators_most_transfers():
    try:
        # Llamar a la función de mysqlfunc para obtener los operadores con más traslados
        results = get_operators_with_most_transfers()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

@main_bp.route("/traslado/porcentajes", methods=['GET'])
def monthly_transfer_percentages():
    try:
        # Llamar a la función de mysqlfunc para obtener los porcentajes mensuales de traslados
        results = get_monthly_transfer_percentages()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

@main_bp.route("/traslado/statussemana", methods=['GET'])
def weekly_transfer_status():
    try:
        # Llamar a la función de mysqlfunc para obtener el estado semanal de los traslados
        results = get_weekly_transfer_status()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


# GET endpoint to just get the number of last km trip
# Call /viaje/kmAmbPrev?IdAmbulancia=1 change the 1 tho
@main_bp.route("/viaje/kmAmbPrev", methods=['GET'])
def kmAmbPrev():
    IdAmbulancia = request.args.get('IdAmbulancia', type=int)
    if IdAmbulancia is None:
        return make_response(jsonify({"error": "IdAmbulancia query parameter is required"}), 400)
    try:
        # Uses rows entcs como lista de diccionarios, can get full row o nomas los km como rows[0]['fKmFinal]
        rows = sql_read_last_amt_km(IdAmbulancia)
        return make_response(jsonify(rows))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


# PUT Starts the trip by setting initial km and setting status as 2, viaje tiene q ya existir para funcionar Moni
#Postman structure: 
#{
#    "IdViaje": 5,
#    "IdTraslado": 7,
#   "fKmInicio": 10000
#  }
@main_bp.route("/viaje/iniciar", methods=['PUT'])
def iniciar_viaje():
    # Many params so extract el request
    data = request.json

    IdViaje = data.get('IdViaje')
    IdTraslado = data.get('IdTraslado')
    fKmInicio = data.get('fKmInicio')
    
    if IdViaje is None or IdTraslado is None or fKmInicio is None:
        return make_response(jsonify({
            "error": "Faltan params: IdViaje, IdTraslado, fKmInicio"
        }), 400)
    
    try:
        result = sql_update_start_trip(IdViaje, IdTraslado, fKmInicio) # Can call it so starts 
        return make_response(jsonify(result), 200) # Success result 
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500) # Fracaso

# PUT Starts the trip by setting initial km and setting status as 2, viaje tiene q ya existir para funcionar Moni
@main_bp.route("/viaje/finalizar", methods=['PUT'])
def finalizar_viaje():
    # Many params so extract el request
    data = request.json

    IdViaje = data.get('IdViaje')
    IdTraslado = data.get('IdTraslado')
    fKmInicio = data.get('fKmInicio')
    fKmFinal = data.get('fKmFinal')
    
    if IdViaje is None or IdTraslado is None or fKmInicio is None or fKmFinal is None:
        return make_response(jsonify({
            "error": "Faltan params: IdViaje, IdTraslado, fKmInicio, fKmFinal"
        }), 400)
    
    try:
        result = sql_update_end_trip(IdViaje, IdTraslado, fKmInicio, fKmFinal) # Can call it so starts 
        return make_response(jsonify(result), 200) # Success result 
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500) # Fracaso
    
# ENDPOINTS PARA SOLICITAR VIAJE

# GET todas las ubicaciones
@main_bp.route("/api/ubicaciones", methods=['GET'])
def get_ubicaciones():
    try:
        ubicaciones = sql_read_all('Ubicacion')
        return make_response(jsonify(ubicaciones))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET todos los operadores (solo IdTipoPersonal = 1)
@main_bp.route("/api/operadores", methods=['GET'])
def get_operadores():
    try:
        operadores = sql_read_where('Usuarios', {'IdTipoPersonal': 1})
        return make_response(jsonify(operadores))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET tipos de traslado
@main_bp.route("/api/tipostraslado", methods=['GET'])
def get_tipos_traslado():
    try:
        tipos = sql_read_all('TipoTraslado')
        return make_response(jsonify(tipos))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# GET estatus
@main_bp.route("/api/estatus", methods=['GET'])
def get_estatus():
    try:
        estatus = sql_read_all('Estatus')
        return make_response(jsonify(estatus))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET socio por número
@main_bp.route("/api/socios/<int:numeroSocio>", methods=['GET'])
def get_socio(numeroSocio):
    try:
        socio = sql_read_where('Socios', {'IdNumeroSocio': numeroSocio})
        if socio:
            return make_response(jsonify(socio[0]))
        else:
            return make_response(jsonify({'error': 'Socio no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


#Enpoint para actualizar el km
@main_bp.route("/viaje/km", methods=['PUT'])
def viajekm_update():
    d = request.json
    
    d_field = {}
    if 'fKmInicio' in d:
        d_field['fKmInicio'] = d['fKmInicio']
    else:
        d_field['fKmFinal'] = d['fKmFinal']

    d_where = {'IdTraslado': d['IdTraslado']}
    sql_update_where('Viaje', d_field, d_where)
    return make_response(jsonify('ok'))
