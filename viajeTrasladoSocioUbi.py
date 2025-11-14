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