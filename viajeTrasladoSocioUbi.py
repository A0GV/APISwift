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