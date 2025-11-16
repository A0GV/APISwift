cnx = None
mssql_params = {}

def mssql_connect(sql_creds):
    import pymssql
    cnx = pymssql.connect(
        server=sql_creds['DB_HOST'],
        user=sql_creds['DB_USER'],
        password=sql_creds['DB_PASSWORD'],
        database=sql_creds['DB_NAME'])
    return cnx

# Obitiene km de viaje más reciente terminado de la ambulancia
def sql_read_last_amt_km(IdAmbulancia):
    import pymssql
    global cnx, mssql_params
    
    # Raw query, just parameterized
    read = """
        SELECT TOP (1) fKmFinal
        FROM Viaje v
        JOIN Traslado t ON t.IdTraslado = v.IdTraslado 
        WHERE v.IdAmbulancia = %s
          AND v.fKmFinal IS NOT NULL
        ORDER BY v.dtFechaFin DESC;
    """ # Sending the %s as the ambuance id 

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdAmbulancia,)) # Adds ambulance id 
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdAmbulancia,))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_last_amt_km:%s" % e)

# All of the trip info. like, all of it, given the traslado info
def sql_read_trip_details(IdTraslado):
    import pymssql
    global cnx, mssql_params
    
    # Raw query, just parameterized
    read = """
        SELECT
            v.*,
            t.*,
            s.*,
            uo.vcDomicilio AS OrigenDomicilio,
            uo.fLatitud AS OrigenLatitud,
            uo.fLongitud AS OrigenLongitud,
            ud.vcDomicilio AS DestinoDomicilio,
            ud.fLatitud AS DestinoLatitud,
            ud.fLongitud AS DestinoLongitud,
            tt.* 
        FROM Viaje v
        JOIN Traslado t ON t.IdTraslado = v.IdTraslado
        JOIN Socios s ON t.IdNumeroSocio = s.IdNumeroSocio 
        JOIN Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
        JOIN Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
        JOIN TipoTraslado tt ON t.IdTipoTraslado = tt.IdTipoTraslado
        WHERE v.IdTraslado = %s;
        """ # Sending the %s as the traslado id 

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdTraslado,)) # Adds ambulance id 
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdTraslado,))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_trip_details:%s" % e)

# PUT usa IdTraslado para set fKmInicial a curr value y change IdEstatus a 2
def sql_update_start_trip(IdViaje, IdTraslado, fKmInicio):
    import pymssql
    global cnx, mssql_params
    
    # Hace dos queries pq tenemos div km y estatus, so sets initial km and changes from 1 -> 2
    update_queries = """
        UPDATE Viaje
        SET fKmInicio = %s
        WHERE IdViaje = %s;
        
        UPDATE Traslado
        SET IdEstatus = 2
        WHERE IdTraslado = %s;
    """
    
    try:
        try:
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, IdViaje, IdTraslado))
            cnx.commit()  # Execs los dos cambios
            cursor.close()
            return {"success": True, "message": "Started trip by setting initial km and changing status"}
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, IdViaje, IdTraslado))
            cnx.commit()
            cursor.close()
            return {"success": True, "message": "Started trip con km y estatus"}
    except Exception as e:
        raise TypeError("sql_update_start_trip: %s" % e)

# PUT usa IdTraslado para set fKmInicial y fKmFinal a curr value y change IdEstatus a 3
def sql_update_end_trip(IdViaje, IdTraslado, fKmInicio, fKmFinal):
    import pymssql
    global cnx, mssql_params
    
    # Hace dos queries pq tenemos div km y estatus, so sets initial, final km and then in traslado marca como completado
    update_queries = """
        UPDATE Viaje 
        SET fKmInicio = %s, fKmFinal = %s
        WHERE IdViaje = %s;
        
        UPDATE Traslado
        SET IdEstatus = 3
        WHERE IdTraslado = %s;
    """
    
    try:
        try:
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, fKmFinal, IdViaje, IdTraslado))
        
            cnx.commit()  # Execs los dos cambios
            cursor.close()
            return {"success": True, "message": "Ended trip, set initial km and changed status"}
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, fKmFinal, IdViaje, IdTraslado))
            
            cnx.commit()
            cursor.close()
            return {"success": True, "message": "Ended trip con km y estatus"}
    except Exception as e:
        raise TypeError("sql_update_end_trip: %s" % e)






def sql_read_next_trip(id_operador):
    import pymssql
    global cnx, mssql_params

    query = """
        SELECT TOP (1)
            v.IdViaje,
            v.dtFechaInicio,
            v.dtFechaFin,
            v.IdAmbulancia,
            t.IdTraslado,
            t.IdEstatus,
            t.vcRazon,
            uo.vcDomicilio AS vcOrigen,     -- Cambié a vcOrigen para que coincida con el modelo
            ud.vcDomicilio AS vcDestino,   -- Cambié a vcDestino para que coincida con el modelo
            CONCAT(s.vcNombre, ' ', s.vcApellidoPaterno, ' ', ISNULL(s.vcApellidoMaterno, '')) AS vcPaciente
        FROM Traslado t
        LEFT JOIN Viaje v ON v.IdTraslado = t.IdTraslado
        LEFT JOIN Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
        LEFT JOIN Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
        LEFT JOIN Socios s ON t.IdNumeroSocio = s.IdNumeroSocio  -- Agregué JOIN para el paciente
        WHERE t.IdEstatus = 1
          AND t.IdUsuarioOperador = %s
        ORDER BY t.dtFechaCreacion ASC;
    """

    try:
        cursor = cnx.cursor(as_dict=True)
        cursor.execute(query, (id_operador,))
    except pymssql._pymssql.InterfaceError:
        print("Reconnecting to SQL Server...")
        cnx = mssql_connect(mssql_params)
        cursor = cnx.cursor(as_dict=True)
        cursor.execute(query, (id_operador,))

    try:
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        raise TypeError(f"sql_read_next_trip: {e}")






if __name__ == '__main__':
    import json
    mssql_params = {}
    mssql_params['DB_HOST'] = '100.80.80.7'
    mssql_params['DB_NAME'] = 'nova'
    mssql_params['DB_USER'] = 'SA'
    mssql_params['DB_PASSWORD'] = 'Shakira123.'
    cnx = mssql_connect(mssql_params)
    
    # Prueba simple con una tabla que existe
    try:
        print("Probando conexión con tabla Usuarios...")
        rx = sql_read_all('Usuarios')
        print(json.dumps(rx, indent=4, default=str))
        print("\nConexión exitosa!")
    except Exception as e:
        print(f"Error: {e}")
    
    cnx.close()

if __name__ == '__main__':
    import json
    mssql_params = {}
    mssql_params['DB_HOST'] = '100.80.80.7'
    mssql_params['DB_NAME'] = 'nova'
    mssql_params['DB_USER'] = 'SA'
    mssql_params['DB_PASSWORD'] = 'Shakira123.'
    cnx = mssql_connect(mssql_params)
    
    # Prueba simple con una tabla que existe
    try:
        print("Probando conexión con tabla Usuarios...")
        rx = sql_read_all('Usuarios')
        print(json.dumps(rx, indent=4, default=str))
        print("\nConexión exitosa!")
    except Exception as e:
        print(f"Error: {e}")
    
    cnx.close()