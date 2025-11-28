from ..extensions import db
import pymssql

def sql_read_next_trip(id_operador):
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
        cnx = db.get_mssql_connection()
        cursor = cnx.cursor(as_dict=True)
        cursor.execute(query, (id_operador,))
    except pymssql._pymssql.InterfaceError:
        print("Reconnecting to SQL Server...")
        cnx = db.reconnect()
        cursor = cnx.cursor(as_dict=True)
        cursor.execute(query, (id_operador,))

    try:
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        raise TypeError(f"sql_read_next_trip: {e}")
    