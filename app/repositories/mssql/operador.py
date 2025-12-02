from ...extensions import db
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
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (id_operador,))

        except pymssql._pymssql.InterfaceError:
            print("Reconnecting to SQL Server...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (id_operador,))
            
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        raise TypeError(f"sql_read_next_trip: {e}")

def get_user_data(idOperador):
    query = """
            SELECT 
                CONCAT(o.vcNombre, ' ', o.vcApellidoPaterno, ' ', o.vcApellidoMaterno) AS nombre,
                o.vcApodo AS apodo,
                o.vcFotoPerfil AS fotoUrlBase
            FROM Usuarios o
            WHERE o.IdUsuario = %s
            AND o.IdTipoPersonal = 1
    """
    
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (idOperador,))
        except pymssql._pymssql.InterfaceError:
            print("Reconnecting to SQL Server...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (idOperador,))
        result = cursor.fetchone()
        print(result)
        cursor.close()
        return result
    except Exception as e:
        raise TypeError("get_user_data: %s" % e)
     
def post_user_config(idOperador, apodo, fotoUrl, deleteOutdatedUrl=False):
    if fotoUrl is not None:
        query = """
        UPDATE Usuarios
        SET vcApodo = %s , 
            vcFotoPerfil = %s
        WHERE IdUsuario = %s
        """
    elif deleteOutdatedUrl:
        # Aquí no debería existir una foto
        query = """
        UPDATE Usuarios
        SET vcApodo = %s , 
            vcFotoPerfil = NULL
        WHERE IdUsuario = %s
        """
    else:
        query = """
        UPDATE Usuarios
        SET vcApodo = %s 
        WHERE IdUsuario = %s
        """
    
    print(idOperador, apodo, fotoUrl)
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)

        except pymssql._pymssql.InterfaceError:
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)

        if fotoUrl is not None:
            cursor.execute(query, (apodo, fotoUrl, idOperador))
        else:
            cursor.execute(query, (apodo, idOperador))
                
        cnx.commit()
        cursor.close()

    except Exception as e:
        raise TypeError(f"sql_post_user_config: {e}")