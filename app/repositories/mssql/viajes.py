import pymssql
from ...extensions import db

def get_viaje_completo(id_viaje):
    query = """
        SELECT 
            v.IdViaje,
            CONVERT(VARCHAR, v.dtFechaInicio, 126) as dtFechaInicio,
            CONVERT(VARCHAR, v.dtFechaFin, 126) as dtFechaFin,
            v.IdAmbulancia,
            v.fKmInicio,
            v.fKmFinal,
            v.IdTraslado,
            v.IdNumeroSocio,
            t.IdUsuarioOperador,
            t.IdTipoTraslado,
            t.IdUbiOrigen,
            t.IdUbiDest,
            t.vcRazon,
            t.IdEstatus,
            e.vcEstatus,
            ta.tipo as vcTipoAmbulancia
        FROM Viaje v
        INNER JOIN Traslado t ON v.IdTraslado = t.IdTraslado
        INNER JOIN Estatus e ON t.IdEstatus = e.IdEstatus
        INNER JOIN Ambulancia a ON v.IdAmbulancia = a.IdAmbulancia
        INNER JOIN tipoAmbulancia ta ON a.IdTipoAmb = ta.IdTipoAmb
        WHERE v.IdViaje = %d
    """ % id_viaje
    
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
        return result
    except Exception as e:
        raise TypeError("get_viaje_completo: %s" % e)

def actualizar_viaje_completo(id_viaje, data):
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
        
        # Primero obtener el IdTraslado asociado al viaje
        cursor.execute("SELECT IdTraslado FROM Viaje WHERE IdViaje = %d" % id_viaje)
        viaje = cursor.fetchone()
        
        if not viaje:
            cursor.close()
            return None
        
        id_traslado = viaje['IdTraslado']
        
        # Determinar el estatus basado en kilometrajes
        km_inicio = data.get('fKmInicio', None)
        km_final = data.get('fKmFinal', None)
        
        if km_inicio is None and km_final is None:
            id_estatus = 1  # Solicitado
        elif km_inicio is not None and km_final is None:
            id_estatus = 2  # En proceso
        else:
            id_estatus = 3  # Terminado
        
        # Actualizar tabla Viaje
        update_viaje = """
            UPDATE Viaje SET
                dtFechaInicio = '%s',
                dtFechaFin = '%s',
                IdAmbulancia = %d,
                fKmInicio = %s,
                fKmFinal = %s,
                IdNumeroSocio = %d
            WHERE IdViaje = %d
        """ % (
            data['dtFechaInicio'],
            data['dtFechaFin'],
            data['IdAmbulancia'],
            km_inicio if km_inicio else 'NULL',
            km_final if km_final else 'NULL',
            data['IdNumeroSocio'],
            id_viaje
        )
        cursor.execute(update_viaje)
        
        # Actualizar tabla Traslado (incluyendo el estatus calculado)
        update_traslado = """
            UPDATE Traslado SET
                IdUsuarioOperador = %d,
                IdNumeroSocio = %d,
                IdTipoTraslado = %d,
                IdUbiOrigen = %d,
                IdUbiDest = %d,
                vcRazon = '%s',
                IdEstatus = %d
            WHERE IdTraslado = %d
        """ % (
            data['IdUsuarioOperador'],
            data['IdNumeroSocio'],
            data['IdTipoTraslado'],
            data['IdUbiOrigen'],
            data['IdUbiDest'],
            data['vcRazon'].replace("'", "''"),
            id_estatus,
            id_traslado
        )
        cursor.execute(update_traslado)
        
        cnx.commit()
        cursor.close()
        
        return {'IdViaje': id_viaje, 'IdTraslado': id_traslado, 'message': 'Actualizado exitosamente'}
        
    except Exception as e:
        raise TypeError("actualizar_viaje_completo: %s" % e)

def cancelar_viaje(id_viaje):
    
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
        
        # Obtener el IdTraslado asociado al viaje
        cursor.execute("SELECT IdTraslado FROM Viaje WHERE IdViaje = %d" % id_viaje)
        viaje = cursor.fetchone()
        
        if not viaje:
            cursor.close()
            return None
        
        id_traslado = viaje['IdTraslado']
        
        # Cambiar estatus a 4 (Cancelado)
        cursor.execute("UPDATE Traslado SET IdEstatus = 4 WHERE IdTraslado = %d" % id_traslado)
        
        cnx.commit()
        cursor.close()
        
        return {'IdViaje': id_viaje, 'IdTraslado': id_traslado, 'message': 'Viaje cancelado exitosamente'}
        
    except Exception as e:
        raise TypeError("cancelar_viaje: %s" % e)
