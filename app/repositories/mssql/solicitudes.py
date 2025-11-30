from ...extensions import db
from .mysqlfunc import sql_insert_row_into
import pymssql

def get_proximo_numero_solicitud():
    query = "SELECT MAX(IdTraslado) as ultimoId FROM Traslado"
    
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        # Si no hay traslados, empezar en 1, si no, sumar 1 al último
        ultimo_id = result['ultimoId'] if result['ultimoId'] is not None else 0
        proximo_numero = ultimo_id + 1
        
        return proximo_numero
    except Exception as e:
        raise TypeError("get_proximo_numero_solicitud: %s" % e)
    


def crear_solicitud_completa(data):
    from datetime import datetime
    
    try:
        # Insertar en Traslado
        traslado_data = {
            'IdUsuarioOperador': data['IdUsuarioOperador'],
            'IdNumeroSocio': data['IdNumeroSocio'],
            'IdTipoTraslado': data['IdTipoTraslado'],
            'IdUbiOrigen': data['IdUbiOrigen'],
            'IdUbiDest': data['IdUbiDest'],
            'vcRazon': data['vcRazon'],
            'IdEstatus': 1,  # Siempre empieza como "Solicitado"
            'dtFechaCreacion': datetime.now()  # Agregar fecha de creación
        }
        
        id_traslado = sql_insert_row_into('Traslado', traslado_data)
        
        if not id_traslado:
            raise Exception('No se pudo crear el traslado')
        
        # Insertar en Viaje
        viaje_data = {
            'IdUsuarioCoord': data['IdUsuarioCoord'],
            'dtFechaInicio': data['dtFechaInicio'],
            'dtFechaFin': data['dtFechaFin'],
            'IdAmbulancia': data['IdAmbulancia'],
            'fKmInicio': None,
            'fKmFinal': None,
            'IdTraslado': id_traslado,
            'IdNumeroSocio': data['IdNumeroSocio']
        }
        
        id_viaje = sql_insert_row_into('Viaje', viaje_data)
        
        if not id_viaje:
            raise Exception('No se pudo crear el viaje')
        
        # Retornar resultado
        return {
            'IdTraslado': int(id_traslado),
            'IdViaje': int(id_viaje),
            'message': 'Solicitud creada exitosamente'
        }
        
    except Exception as e:
        raise TypeError("crear_solicitud_completa: %s" % e)