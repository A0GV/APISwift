from flask import Blueprint, jsonify, request, make_response
from pydantic import ValidationError
from ..repositories.mssql.traslados import get_traslados, get_tras_dias, get_tras_dias_2, sql_read_trip_details, get_estatus_tras, get_completados, sql_read_today_coordi
from flask_jwt_extended import jwt_required
from ..models.roles import role_required
from app.extensions import limiter

traslados_bp = Blueprint("traslados", __name__, url_prefix="/api/traslados")

# NO FUNCIONARÁ AHORITA PORQUE EN SWIFT, LA LLAMADA NO TIENE EL /API AL INICIO
@traslados_bp.route("/", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def traslados():
    try:
        try:
            # Importar la clase para validación de tipos
            from models import TrasladoQueryParams
            validated_params = TrasladoQueryParams(**request.args.to_dict())
            params = validated_params.model_dump(exclude_none=True)

        except ValidationError as e:
            return make_response(jsonify({
                "error": "Parámetros de consulta inválidos",
                "code": 400,
                "details":str(e.errors())}), 400)
        try:
            # Descarta operador y paciente si se quiere hacer una búsqueda or entre ambos
            if params.get('orPacienteOperador') is not None and params['orPacienteOperador']:
                if params.get('operador') is None or params.get('paciente') is None:
                    raise ValueError("Si el parámetro de consulta 'orPacienteOperador' está presente, los parámetros 'operador' y 'paciente' deben estar presentes.")

                # Se combinan en una consulta y se eliminan las originales
                params['orPacienteOperador'] = [params['operador'], params['paciente']]
                params.pop('operador')
                params.pop('paciente')

        except Exception as e:
            return make_response(jsonify({
                "error": str(e),
                "code": 400}), 400)
        
        # Se llama al servicio de base de datos
        results = get_traslados(params)
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)
        


#Sacar el traslado por el dia
#/tdia?date='2025-11-14'&&idOperador=2
@traslados_bp.route("/tdia", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def diaTras():
    try:
        date = request.args.get('date')             
        idOperador = request.args.get('idOperador') 
        
        ovj = get_tras_dias(date, idOperador)
        #Para regresar la lista aunque este vacia porque lo del operador se checa en backend no aqui
        #Pero al regresar una vacia, se checa si esta empty la lista que recibe y ya 
        return make_response(jsonify(ovj))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

@traslados_bp.route("/tdia/<int:idOperador>", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def diaTras2(idOperador):
    try:
        date = request.args.get('date')             
        ovj = get_tras_dias_2(date, idOperador)
        #Para regresar la lista aunque este vacia porque lo del operador se checa en backend no aqui
        #Pero al regresar una vacia, se checa si esta empty la lista que recibe y ya 

        if(ovj is None):
            return make_response(jsonify({}))
        ovj["activo"] = ovj["estatus"] == "En proceso"
        return make_response(jsonify(ovj))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
    

# GET para los detalles de un traslado
# Call /traslado/detallesTraslado?IdTraslado=? change the ?
@traslados_bp.route("/detalles", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def detallesTraslado():
    IdTraslado = request.args.get('IdTraslado', type=int)
    if IdTraslado is None:
        return make_response(jsonify({"error": "IdTraslado query parameter is required"}), 400)
    try:
        # Uses rows entcs como lista de diccionarios
        rows = sql_read_trip_details(IdTraslado)
        return make_response(jsonify(rows))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)
    
#/completados?dateinicio=2025-11-01&datefinal=2025-11-18&idOperador=2
@traslados_bp.route("/completados", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def get_completador():
    try:
        dateinicio = request.args.get('dateinicio')
        datefinal = request.args.get('datefinal')
        idOperador = request.args.get('idOperador')
        
        params = {}

        if dateinicio and datefinal:
            params["dateinicio"] = dateinicio
            params["datefinal"] = datefinal

        if idOperador:
            params["idOperador"] = idOperador

        ovj = get_completados(params)
        return make_response(jsonify(ovj))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


#/estatus?date=2025-11-18&idOperador=2
@traslados_bp.route("/estatus", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def get_esta_tras():
    try:
        date = request.args.get('date')             
        idOperador = request.args.get('idOperador') 
        
        ovj = get_estatus_tras(date, idOperador)
        return make_response(jsonify(ovj))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# /viajesCoord?date=2025-11-28
@traslados_bp.route("/viajesCoord", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def viajesPrev():
    '''date = request.args.get('date', type=str)
    if date is None:
        return make_response(jsonify({"error": "date query parameter is required"}), 400)
    try:
        rows = home.sql_read_today_coordi(date)
        return make_response(jsonify(rows))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)'''
    try:
        date = request.args.get('date')             
        ovj = sql_read_today_coordi(date)
        return make_response(jsonify(ovj))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)