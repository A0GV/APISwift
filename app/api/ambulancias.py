from flask import Blueprint, jsonify, request, make_response
from ..repositories.mssql.ambulancias import get_maintenance, sql_get_ambulancias_disponibles, get_tipo_ambulancia_por_id, sql_check_ambulancia_status
from ..repositories.mssql.operador import sql_read_next_trip
from flask_jwt_extended import jwt_required
from ..models.roles import role_required
from app.extensions import limiter

ambulancias_bp = Blueprint("ambulancias", __name__, url_prefix="/api/ambulancias")


@ambulancias_bp.route("/mantenimiento", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def dMantenimiento():
    try:
        # Llamar a la función de mysqlfunc para obtener el estado semanal de los traslados
        results = get_maintenance()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


@ambulancias_bp.route("/siguiente-traslado", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
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
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def get_ambulancias_disponibles():
    try:
        fecha_inicio = request.args.get('fechaInicio', None)
        fecha_fin = request.args.get('fechaFin', None)
        excluir_viaje = request.args.get('excluirViaje', None)

        # Validación de tamaño y tipo
        if not fecha_inicio or not fecha_fin:
            return make_response(jsonify({'error': 'Se requieren fechaInicio y fechaFin'}), 400)
        if len(fecha_inicio) > 30 or len(fecha_fin) > 30:
            return make_response(jsonify({'error': 'fechaInicio o fechaFin demasiado largas'}), 400)
        if excluir_viaje and (not excluir_viaje.isdigit() or len(excluir_viaje) > 10):
            return make_response(jsonify({'error': 'excluirViaje inválido'}), 400)

        excluir_viaje_int = int(excluir_viaje) if excluir_viaje else None

        ambulancias = sql_get_ambulancias_disponibles(fecha_inicio, fecha_fin, excluir_viaje_int)
        return make_response(jsonify(ambulancias))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
    
# GET tipo de ambulancia por ID de ambulancia
@ambulancias_bp.route("/<int:idAmbulancia>/tipo", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
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

# GET de ubicaciones probables de ambulancias activas ahorita en base al tiempo y status
@ambulancias_bp.route("/status", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def ambulanciaStatus():
    IdAmbulancia = request.args.get('IdAmbulancia', type=str)
    FechaActual = request.args.get('FechaActual', type=str)

    # Validación de tamaño y tipo
    if IdAmbulancia is None:
        return make_response(jsonify({"error": "IdAmbulancia query parameter is required"}), 400)
    if not IdAmbulancia.isdigit() or len(IdAmbulancia) > 10:
        return make_response(jsonify({"error": "IdAmbulancia inválido"}), 400)
    if FechaActual is None:
        return make_response(jsonify({"error": "FechaActual query parameter is required"}), 400)
    if len(FechaActual) > 40:
        return make_response(jsonify({"error": "FechaActual demasiado larga"}), 400)

    IdAmbulancia_int = int(IdAmbulancia)

    try:
        rows = sql_check_ambulancia_status(IdAmbulancia_int, FechaActual)

        # Si no hay filas, ambulancia no está en viaje activo entcs va a decir q está en OnTrip 0 y usa coords de nova
        if len(rows) == 0:
            return make_response(jsonify({
                "OnTrip": 0,
                "IdAmbulancia": IdAmbulancia_int,
                "DefaultLat": 25.7320824,  # Nova coords
                "DefaultLong": -100.3027483
            }))
        else:
            # Viaje activo 1 sí regresa viaje itself pero customized json en vez de straight up sql server
            row = rows[0]
            return make_response(jsonify({
                "OnTrip": 1,
                "IdAmbulancia": IdAmbulancia_int,
                "IdViaje": row['IdViaje'],
                "OrigenLat": float(row['OrigenLat']) if row['OrigenLat'] else None,
                "OrigenLong": float(row['OrigenLong']) if row['OrigenLong'] else None,
                "DestinoLat": float(row['DestinoLat']) if row['DestinoLat'] else None,
                "DestinoLong": float(row['DestinoLong']) if row['DestinoLong'] else None,
                "OrigenDomicilio": row['OrigenDomicilio'],
                "DestinoDomicilio": row['DestinoDomicilio'],
                "IdEstatus": row['IdEstatus'],
                "IdTraslado": row['IdTraslado'],
                "dtFechaInicio": row['dtFechaInicio'],
                "dtFechaFin": row['dtFechaFin'],
                "OrigenId": row['OrigenId'],
                "DestinoId": row['DestinoId']
            }))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)