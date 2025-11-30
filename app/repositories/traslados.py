import pymssql
from ..extensions import db

def get_tras_dias(date, idOperador):
    query = """
        SELECT
            v.dtFechaInicio AS inicio,
            v.dtFechaFin AS fin,
            v.idAmbulancia AS amb,
            v.IdViaje as viaje,
            v.fKmInicio,
            v.fKmFinal,
            uo.vcDomicilio AS OrigenDomicilio,
            ud.vcDomicilio AS DestinoDomicilio,
            t.IdTraslado AS traslado,
            e.vcEstatus as estatus,
            tp.vcTipo AS tipo,
            s.vcNombre AS nombreSocio,
            s.vcApellidoPaterno AS apellido1,
            s.vcApellidoMaterno AS apellido2
        FROM dbo.Viaje v
        JOIN dbo.Traslado t ON t.IdTraslado = v.IdTraslado
        JOIN dbo.TipoTraslado tp ON tp.IdTipoTraslado = t.IdTipoTraslado
        JOIN dbo.Socios s ON t.IdNumeroSocio = s.IdNumeroSocio 
        JOIN dbo.Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
        JOIN dbo.Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
        JOIN dbo.Estatus e ON t.IdEstatus = e.IdEstatus
        WHERE CAST(v.dtFechaInicio AS DATE) = %s 
            AND t.IdUsuarioOperador = %s
            AND e.vcEstatus NOT IN ('Terminado','Cancelado')
        ORDER BY v.dtFechaInicio ASC;
    """

    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        rows = cursor.fetchall()
        cursor.close()
        return rows

    except Exception as e:
        raise TypeError("get_tras_dias:%s" % e)

def get_tras_dias_2(date, idOperador):
    query = """
        SELECT
            v.dtFechaInicio AS inicio,
            v.dtFechaFin AS fin,
            v.IdAmbulancia AS amb,
            v.IdViaje AS viaje, 
            v.fKmInicio AS kmInicial, 
            t.IdTraslado AS traslado,
            e.vcEstatus AS estatus,
            s.vcNombre AS nombreSocio,
            s.vcApellidoPaterno AS apellido1,
            s.vcApellidoMaterno AS apellido2,
            uo.vcDomicilio AS OrigenDomicilio,
            ud.vcDomicilio AS DestinoDomicilio,
            tt.vcTipo AS tipo
        FROM dbo.Viaje v
        JOIN dbo.Traslado t ON t.IdTraslado = v.IdTraslado
        JOIN dbo.Socios s ON t.IdNumeroSocio = s.IdNumeroSocio 
        JOIN dbo.Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
        JOIN dbo.Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
        JOIN dbo.TipoTraslado tt ON t.IdTipoTraslado = tt.IdTipoTraslado
        JOIN dbo.Estatus e ON t.IdEstatus = e.IdEstatus
        WHERE CAST(v.dtFechaInicio AS DATE) = %s 
            AND t.IdUsuarioOperador = %s
            AND e.vcEstatus = 'Solicitado'
            AND v.fKmInicio IS NULL  
        ORDER BY v.dtFechaInicio ASC;

    """
    
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        rows = cursor.fetchall()
        cursor.close()
        if len(rows) == 0:
            return {}
        return rows[0]

    except Exception as e:
        raise TypeError("get_tras_dias:%s" % e)


def get_traslados(params):
    query = """
    SELECT v.idViaje, v.dtFechaInicio, v.dtFechaFin, v.idAmbulancia, CONCAT(s.vcNombre, ' ', s.vcApellidoPaterno, ' ', s.vcApellidoMaterno) AS paciente, e.vcEstatus AS estado, CONCAT(uO.vcNombre, ' ', uO.vcApellidoPaterno, ' ', uO.vcApellidoMaterno) AS operador, CONCAT(uC.vcNombre, ' ', uC.vcApellidoPaterno, ' ', uC.vcApellidoMaterno) AS coordinador, v.fKmInicio, v.fKmFinal
    FROM Viaje v 
    JOIN Traslado t  ON v.idTraslado = t.idTraslado
    JOIN Socios s    ON s.IdNumeroSocio = t.IdNumeroSocio 
    JOIN Usuarios uC ON uC.IdUsuario = v.IdUsuarioCoord 
    JOIN Usuarios uO ON uO.IdUsuario = t.IdUsuarioOperador
    JOIN Estatus e   ON e.IdEstatus = t.IdEstatus
    WHERE 1 = 1
    """

    filters = {
        'fechaInicio' : ["AND v.dtFechaInicio >= %s", lambda x: x],
        'fechaFin' : ["AND v.dtFechaInicio <= %s", lambda x: x],
        'estado' : ["AND e.vcEstatus LIKE %s", lambda x: x],
        'paciente' : ["AND LOWER(CONCAT(s.vcNombre, ' ', s.vcApellidoPaterno, ' ', s.vcApellidoMaterno)) COLLATE Latin1_General_CI_AI LIKE LOWER(%s)", lambda x: "%" + x + "%"],
        'operador' : ["AND LOWER (CONCAT(uO.vcNombre, ' ', uO.vcApellidoPaterno, ' ', uO.vcApellidoMaterno)) COLLATE Latin1_General_CI_AI LIKE LOWER(%s)", lambda x: "%" + x + "%"],
        'idAmbulancia' : ["AND v.IdAmbulancia = %s", lambda x: x],
        'orPacienteOperador' : ["AND (LOWER(CONCAT(s.vcNombre, ' ', s.vcApellidoPaterno, ' ', s.vcApellidoMaterno)) COLLATE Latin1_General_CI_AI LIKE LOWER(%s) OR LOWER(CONCAT(uO.vcNombre, ' ', uO.vcApellidoPaterno, ' ', uO.vcApellidoMaterno)) COLLATE Latin1_General_CI_AI LIKE LOWER(%s))", lambda pairOP: ["%" + pairOP[0] + "%", "%" + pairOP[1] + "%"]],
    }

    conditions = []
    values = []

    for key, val in params.items():
        condition, fun = filters[key]
        conditions.append(condition)
        res = fun(val)
        if isinstance(res, list):
            values.extend(res)
        else:
            values.append(fun(res))
    
    query += ' '.join(conditions)

    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, tuple(values))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        raise TypeError("get_traslados:%s" % e)
    


# All of the trip info. like, all of it, given the traslado info
def sql_read_trip_details(IdTraslado):
    
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
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdTraslado,)) # Adds ambulance id 
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdTraslado,))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_trip_details:%s" % e)

# PUT usa IdTraslado para set fKmInicial a curr value y change IdEstatus a 2
def sql_update_start_trip(IdViaje, IdTraslado, fKmInicio):

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
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, IdViaje, IdTraslado))
            cnx.commit()  # Execs los dos cambios
            cursor.close()
            return {"success": True, "message": "Started trip by setting initial km and changing status"}
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, IdViaje, IdTraslado))
            cnx.commit()
            cursor.close()
            return {"success": True, "message": "Started trip con km y estatus"}
    except Exception as e:
        raise TypeError("sql_update_start_trip: %s" % e)

# PUT usa IdTraslado para set fKmInicial y fKmFinal a curr value y change IdEstatus a 3
def sql_update_end_trip(IdViaje, IdTraslado, fKmInicio, fKmFinal):
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
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, fKmFinal, IdViaje, IdTraslado))
        
            cnx.commit()  # Execs los dos cambios
            cursor.close()
            return {"success": True, "message": "Ended trip, set initial km and changed status"}
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmInicio, fKmFinal, IdViaje, IdTraslado))
            
            cnx.commit()
            cursor.close()
            return {"success": True, "message": "Ended trip con km y estatus"}
    except Exception as e:
        raise TypeError("sql_update_end_trip: %s" % e)


# PUT usa IdTraslado para set  fKmFinal a curr value y change IdEstatus a 3
def sql_update_quick_end(IdViaje, IdTraslado, fKmFinal):
    # Hace dos queries pq tenemos div km y estatus, so sets initial, final km and then in traslado marca como completado
    update_queries = """
        UPDATE Viaje 
        SETfKmFinal = %s
        WHERE IdViaje = %s;
        
        UPDATE Traslado
        SET IdEstatus = 3
        WHERE IdTraslado = %s;
    """
    
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmFinal, IdViaje, IdTraslado))
        
            cnx.commit()  # Execs los dos cambios
            cursor.close()
            return {"success": True, "message": "Ended trip, set initial km and changed status"}
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor()
            cursor.execute(update_queries, (fKmFinal, IdViaje, IdTraslado))
            
            cnx.commit()
            cursor.close()
            return {"success": True, "message": "Ended trip con km final y estatus"}
    except Exception as e:
        raise TypeError("sql_update_quick_end: %s" % e)

def get_completados(params):
    query = """
    SELECT COUNT(*) AS TotalTraslados
    FROM dbo.Viaje v
    JOIN dbo.Traslado t ON t.IdTraslado = v.IdTraslado
    WHERE t.IdEstatus = 2
    """
    conditions = []
    values = []

    if "dateinicio" in params and "datefinal" in params:
        conditions.append("AND CAST(v.dtFechaInicio AS DATE) BETWEEN %s AND %s")
        values.append(params["dateinicio"])
        values.append(params["datefinal"])

    if "idOperador" in params:
        conditions.append("AND t.IdUsuarioOperador = %s")
        values.append(params["idOperador"])

    query += ' '.join(conditions)

    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, tuple(values))

        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, tuple(values))

        result = cursor.fetchone()
        cursor.close()
        return result

    except Exception as e:
        raise TypeError("get_completados:%s" % e)


def get_estatus_tras(date, idOperador):
    query = """
    SELECT 
        e.vcEstatus AS Estatus,
        COUNT(*) AS TotalTraslados
    FROM dbo.Viaje v
    JOIN dbo.Traslado t ON t.IdTraslado = v.IdTraslado
    JOIN dbo.TipoTraslado tp ON tp.IdTipoTraslado = t.IdTipoTraslado
    JOIN dbo.Estatus e ON t.IdEstatus = e.IdEstatus
    WHERE CAST(v.dtFechaInicio AS DATE) = %s
        AND t.IdUsuarioOperador = %s
    GROUP BY e.vcEstatus
    ORDER BY TotalTraslados DESC;
    """
    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (date, idOperador))

        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()

            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (idOperador))

        rows = cursor.fetchall()
        cursor.close()
        if len(rows) == 0:
            return []
        return rows


    except Exception as e:
        raise TypeError("get_estatus_tras:%s" % e)

# Home de coordi
def sql_read_today_coordi(date):

    
    # Raw query, just parameterized
    read = """
        SELECT
            v.IdViaje, -- Para mandar detalle del viaje 
            v.dtFechaInicio,
            v.dtFechaFin,
            v.IdAmbulancia,
            v.IdUsuarioCoord,  
            u.vcNombre, 
            u.vcApellidoPaterno, 
            u.vcApellidoMaterno,
            uo.vcDomicilio AS OrigenDomicilio,
            ud.vcDomicilio AS DestinoDomicilio, 
            t.IdTraslado, 
            e.IdEstatus,
            e.vcEstatus, 
            tp.vcTipo
        FROM dbo.Viaje v
        JOIN dbo.Traslado t ON t.IdTraslado = v.IdTraslado
        JOIN dbo.TipoTraslado tp ON tp.IdTipoTraslado = t.IdTipoTraslado
        JOIN dbo.Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
        JOIN dbo.Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
        JOIN dbo.Estatus e ON t.IdEstatus = e.IdEstatus
        -- Datos de coordi 
        JOIN dbo.Usuarios u ON v.IdUsuarioCoord = u.IdUsuario
        WHERE CAST(v.dtFechaInicio AS DATE) = %s 
            AND e.IdEstatus = 1 
        ORDER BY v.dtFechaInicio ASC;
        """ # Sending the %s as the date

    try:
        try:
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (date,)) # Adds date  
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (date,))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_today_coordi:%s" % e)    