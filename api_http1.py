from flask import Flask, jsonify, make_response, request, send_file
import json
import sys
import mysqlfunc as MSSql
import viajeTrasladoSocioUbi as VTSU
import getCielo as diatras

# Connect to mssql dB from start
mssql_params = {}
mssql_params['DB_HOST'] = '100.80.80.7'
mssql_params['DB_NAME'] = 'nova'
mssql_params['DB_USER'] = 'SA'
mssql_params['DB_PASSWORD'] = 'Shakira123.' 

try:
    MSSql.cnx = MSSql.mssql_connect(mssql_params)
    MSSql.mssql_params = mssql_params
except Exception as e:
    print("Cannot connect to mssql server!: {}".format(e))
    sys.exit()
        
# Connect VTSU Moni
try:
    VTSU.mssql_params = mssql_params # Para reconnect
    VTSU.cnx = VTSU.mssql_connect(mssql_params)
except Exception as e:
    print("Cannot connect to vtsu server!: {}".format(e))
    sys.exit()

#Conectar cielo
try:
    diatras.mssql_params = mssql_params
    diatras.cnx = diatras.mssql_connect(mssql_params)
except Exception as e:
    print("Cannot connect to getCielo server!: {}".format(e))

app = Flask(__name__)

@app.route("/hello")
def hello():
    return "Shakira rocks!\n"

@app.route("/user")
def user():
    username = request.args.get('username', None)
    #print(username)
    d_user = MSSql.read_user_data('users', username)
    return make_response(jsonify(d_user))

@app.route("/crud/create", methods=['POST'])
def crud_create():
    d = request.json
    idUser = MSSql.sql_insert_row_into('users', d)
    return make_response(jsonify(idUser))

@app.route("/crud/read", methods=['GET'])
def crud_read():
    username = request.args.get('username', None)
    d_user = MSSql.sql_read_where('users', {'username': username})
    return make_response(jsonify(d_user))

@app.route("/crud/update", methods=['PUT'])
def crud_update():
    d = request.json
    d_field = {'password': d['password']}
    d_where = {'username': d['username']}
    MSSql.sql_update_where('users', d_field, d_where)
    return make_response(jsonify('ok'))
#apdjaljfnas
@app.route("/crud/delete", methods=['DELETE'])
def crud_delete():
    d = request.json
    d_where = {'username': d['username']}
    MSSql.sql_delete_where('users', d_where)
    return make_response(jsonify('ok'))

@app.route("/demand/hours", methods=['GET'])
def demand_hours():
    try:
        # Llamar a la función de mysqlfunc para obtener los horarios con mayor demanda
        results = MSSql.get_demand_hours()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

@app.route("/operators/most-transfers", methods=['GET'])
def operators_most_transfers():
    try:
        # Llamar a la función de mysqlfunc para obtener los operadores con más traslados
        results = MSSql.get_operators_with_most_transfers()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

@app.route("/transfers/monthly-percentages", methods=['GET'])
def monthly_transfer_percentages():
    try:
        # Llamar a la función de mysqlfunc para obtener los porcentajes mensuales de traslados
        results = MSSql.get_monthly_transfer_percentages()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

@app.route("/transfers/weekly-status", methods=['GET'])
def weekly_transfer_status():
    try:
        # Llamar a la función de mysqlfunc para obtener el estado semanal de los traslados
        results = MSSql.get_weekly_transfer_status()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


# GET endpoint to just get the number of last km trip
# Call /viaje/kmAmbPrev?IdAmbulancia=1 change the 1 tho
@app.route("/viaje/kmAmbPrev", methods=['GET'])
def kmAmbPrev():
    IdAmbulancia = request.args.get('IdAmbulancia', type=int)
    if IdAmbulancia is None:
        return make_response(jsonify({"error": "IdAmbulancia query parameter is required"}), 400)
    try:
        # Uses rows entcs como lista de diccionarios, can get full row o nomas los km como rows[0]['fKmFinal]
        rows = VTSU.sql_read_last_amt_km(IdAmbulancia)
        return make_response(jsonify(rows))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

# GET para los detalles de un traslado
# Call /traslado/detallesTraslado?IdTraslado=? change the ?
@app.route("/traslado/detallesTraslado", methods=['GET'])
def detallesTraslado():
    IdTraslado = request.args.get('IdTraslado', type=int)
    if IdTraslado is None:
        return make_response(jsonify({"error": "IdTraslado query parameter is required"}), 400)
    try:
        # Uses rows entcs como lista de diccionarios
        rows = VTSU.sql_read_trip_details(IdTraslado)
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
@app.route("/viaje/iniciar", methods=['PUT'])
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
        result = VTSU.sql_update_start_trip(IdViaje, IdTraslado, fKmInicio) # Can call it so starts 
        return make_response(jsonify(result), 200) # Success result 
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500) # Fracaso

# PUT Starts the trip by setting initial km and setting status as 2, viaje tiene q ya existir para funcionar Moni
@app.route("/viaje/finalizar", methods=['PUT'])
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
        result = VTSU.sql_update_end_trip(IdViaje, IdTraslado, fKmInicio, fKmFinal) # Can call it so starts 
        return make_response(jsonify(result), 200) # Success result 
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500) # Fracaso
    
# ENDPOINTS PARA SOLICITAR VIAJE

# GET todas las ubicaciones
@app.route("/api/ubicaciones", methods=['GET'])
def get_ubicaciones():
    try:
        ubicaciones = MSSql.sql_read_all('Ubicacion')
        return make_response(jsonify(ubicaciones))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET todos los operadores (solo IdTipoPersonal = 1)
@app.route("/api/operadores", methods=['GET'])
def get_operadores():
    try:
        operadores = MSSql.sql_read_where('Usuarios', {'IdTipoPersonal': 1})
        return make_response(jsonify(operadores))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET tipos de traslado
@app.route("/api/tipostraslado", methods=['GET'])
def get_tipos_traslado():
    try:
        tipos = MSSql.sql_read_all('TipoTraslado')
        return make_response(jsonify(tipos))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET ambulancias disponibles (filtrando por fecha y hora específica)
@app.route("/api/ambulancias/disponibles", methods=['GET'])
def get_ambulancias_disponibles():
    try:
        fecha_inicio = request.args.get('fechaInicio', None)
        fecha_fin = request.args.get('fechaFin', None)
        
        if not fecha_inicio or not fecha_fin:
            return make_response(jsonify({'error': 'Se requieren fechaInicio y fechaFin'}), 400)
        
        # Llamar a la función de mysqlfunc para obtener ambulancias disponibles
        ambulancias = MSSql.get_ambulancias_disponibles(fecha_inicio, fecha_fin)
        return make_response(jsonify(ambulancias))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET tipo de ambulancia por ID de ambulancia
@app.route("/api/ambulancia/<int:idAmbulancia>/tipo", methods=['GET'])
def get_tipo_ambulancia(idAmbulancia):
    try:
        # Llamar a la función de mysqlfunc para obtener el tipo de ambulancia
        tipo = MSSql.get_tipo_ambulancia_por_id(idAmbulancia)
        
        if tipo:
            return make_response(jsonify(tipo))
        else:
            return make_response(jsonify({'error': 'Ambulancia no encontrada'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET estatus
@app.route("/api/estatus", methods=['GET'])
def get_estatus():
    try:
        estatus = MSSql.sql_read_all('Estatus')
        return make_response(jsonify(estatus))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET socio por número
@app.route("/api/socios/<int:numeroSocio>", methods=['GET'])
def get_socio(numeroSocio):
    try:
        socio = MSSql.sql_read_where('Socios', {'IdNumeroSocio': numeroSocio})
        if socio:
            return make_response(jsonify(socio[0]))
        else:
            return make_response(jsonify({'error': 'Socio no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# GET próximo número de solicitud
@app.route("/api/solicitud/proximo-numero", methods=['GET'])
def get_proximo_numero():
    try:
        # Llamar a la función de mysqlfunc para obtener el próximo número
        proximo_numero = MSSql.get_proximo_numero_solicitud()
        return make_response(jsonify({'proximoNumero': proximo_numero}))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


# POST crear solicitud (Traslado + Viaje)
@app.route("/api/solicitud", methods=['POST'])
def crear_solicitud():
    try:
        data = request.json
        
        # Validar datos requeridos
        required_fields = ['IdUsuarioCoord', 'IdUsuarioOperador', 'IdNumeroSocio', 
                          'IdTipoTraslado', 'IdUbiOrigen', 'IdUbiDest', 'vcRazon',
                          'dtFechaInicio', 'dtFechaFin', 'IdAmbulancia']
        
        for field in required_fields:
            if field not in data:
                return make_response(jsonify({'error': f'Falta el campo requerido: {field}'}), 400)
        
        # Llamar a la función de mysqlfunc para crear la solicitud
        result = MSSql.crear_solicitud_completa(data)
        
        return make_response(jsonify(result), 201)
        
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)



#Enpoint para actualizar el km
@app.route("/viaje/km", methods=['PUT'])
def viajekm_update():
    d = request.json
    
    d_field = {}
    if 'fKmInicio' in d:
        d_field['fKmInicio'] = d['fKmInicio']
    else:
        d_field['fKmFinal'] = d['fKmFinal']

    d_where = {'IdTraslado': d['IdTraslado']}
    MSSql.sql_update_where('Viaje', d_field, d_where)
    return make_response(jsonify('ok'))



"""
Endpoints de traslados - Cristian Luque
"""

def queryParams(*claves):
    params = {}
    for clave in claves:
        if request.args.get(clave) is not None:
            params[clave] = request.args.get(clave)
    return params

@app.route("/traslados", methods=['GET'])
def traslados():
    params = queryParams('fechaInit', 'fechaFin', 'estado', 'operador', 'paciente', 'idAmbulancia')

    try:
        results = MSSql.get_traslados(params)
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)
    
    
#Sacar el traslado por el dia
#/tdia?date='2025-11-14'&&idOperador=2
@app.route("/tdia", methods=['GET'])
def diaTras():
    try:
        date = request.args.get('date')             
        idOperador = request.args.get('idOperador') 
        
        ovj = diatras.get_tras_dias(date, idOperador)
        #Para regresar la lista aunque este vacia porque lo del operador se checa en backend no aqui
        #Pero al regresar una vacia, se checa si esta empty la lista que recibe y ya 
        return make_response(jsonify(ovj))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)


@app.route("/ambulancias/mantenimiento", methods=['GET'])
def dMantenimiento():
    try:
        # Llamar a la función de mysqlfunc para obtener el estado semanal de los traslados
        results = MSSql.get_maintenance()
        return make_response(jsonify(results))
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)



if __name__ == '__main__':
    print ("Running API...")
    app.run(host='0.0.0.0', port=10204, debug=True)


