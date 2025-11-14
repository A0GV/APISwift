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

def get_tras_dias(date, idOperador):
    import pymssql
    global cnx, mssql_params

    query = """
        SELECT
            v.dtFechaInicio AS inicio,
            v.dtFechaFin AS fin,
            v.idAmbulancia AS amb,
            uo.vcDomicilio AS OrigenDomicilio,
            ud.vcDomicilio AS DestinoDomicilio,
            tp.vcTipo AS tipo,
            s.vcNombre AS nombreSocio,
            s.vcApellidoPaterno AS apellido1,
            s.vcApellidoMaterno AS apellido2
        FROM Viaje v
        JOIN Traslado t ON t.IdTraslado = v.IdTraslado
        JOIN TipoTraslado tp ON tp.IdTipoTraslado = t.IdTipoTraslado
        JOIN Socios s ON t.IdNumeroSocio = s.IdNumeroSocio 
        JOIN Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
        JOIN Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
        WHERE CAST(v.dtFechaInicio AS DATE) = %s 
          AND t.IdUsuarioOperador = %s;
    """

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)

            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        rows = cursor.fetchall()
        cursor.close()
        return rows

    except Exception as e:
        raise TypeError("get_tras_dias:%s" % e)


if __name__ == '__main__':
    import json

    mssql_params = {}
    mssql_params['DB_HOST'] = '100.80.80.7'
    mssql_params['DB_NAME'] = 'nova'
    mssql_params['DB_USER'] = 'SA'
    mssql_params['DB_PASSWORD'] = 'Shakira123.'

    cnx = mssql_connect(mssql_params)

    try:
        print("Probando get_tras_dias...")
        rx = get_tras_dias("2025-11-14", 2)
        print(json.dumps(rx, indent=4, default=str))
        print("\nConexión exitosa!")
    except Exception as e:
        print(f"Error: {e}")

    cnx.close()
