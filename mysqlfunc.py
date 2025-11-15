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


def read_user_data(table_name, username):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM {} WHERE username = '{}'".format(table_name, username)
    #print(read)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("read_user_data: %s" % e)

def sql_read_all(table_name):
    import pymssql
    global cnx, mssql_params
    read = 'SELECT * FROM %s' % table_name
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_where:%s" % e)


def sql_read_where(table_name, d_where):
    import pymssql
    global cnx, mssql_params
    read = 'SELECT * FROM %s WHERE ' % table_name
    read += '('
    for k,v in d_where.items():
        if v is not None:
            if isinstance(v,bool):
                read += "%s = '%s' AND " % (k,int(v == True))
            else:
                read += "%s = '%s' AND " % (k,v)
        else:
            read += '%s is NULL AND ' % (k)
    # Remove last "AND "
    read = read[:-4]
    read += ')'
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_where:%s" % e)


def sql_insert_row_into(table_name, d):
    import pymssql
    global cnx, mssql_params
    keys = ""
    values = ""
    data = []
    for k in d:
        keys += k + ','
        values += "%s,"
        if isinstance(d[k],bool):
            data.append(int(d[k] == True))
        else:
            data.append(d[k])
    keys = keys[:-1]
    values = values[:-1]
    insert = 'INSERT INTO %s (%s) VALUES (%s)'  % (table_name, keys, values)
    data = tuple(data)
    #print(insert)
    #print(data)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(insert, data)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(insert, data)
        cnx.commit()
        id_new = cursor.lastrowid
        cursor.close()
        return id_new
    except Exception as e:
        raise TypeError("sql_insert_row_into:%s" % e)


def sql_update_where(table_name, d_field, d_where):
    import pymssql
    global cnx, mssql_params
    update = 'UPDATE %s SET ' % table_name
    for k,v in d_field.items():
        if v is None:
            update +='%s = NULL, ' % (k)
        elif isinstance(v,bool):
            update +='%s = %s, ' % (k,int(v == True))
        elif isinstance(v,str):
            update +="%s = '%s', " % (k,v)
        else:
            update +='%s = %s, ' % (k,v)
    # Remove last ", "
    update = update[:-2]
    update += ' WHERE ( '
    for k,v in d_where.items():
        if v is not None:
            if isinstance(v,bool):
                update += "%s = '%s' AND " % (k,int(v == True))
            else:
                update += "%s = '%s' AND " % (k,v)
        else:
            update += '%s is NULL AND ' % (k)
    # Remove last "AND "
    update = update[:-4]
    update += ")"
    #print(update)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(update)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(update)
        cnx.commit()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_update_where:%s" % e)


def sql_delete_where(table_name, d_where):
    import pymssql
    global cnx, mssql_params
    delete = 'DELETE FROM %s ' % table_name
    delete += ' WHERE ( '
    for k,v in d_where.items():
        if v is not None:
            if isinstance(v,bool):
                delete += "%s = '%s' AND " % (k,int(v == True))
            else:
                delete += "%s = '%s' AND " % (k,v)
        else:
            delete += '%s is NULL AND ' % (k)
    # Remove last "AND "
    delete = delete[:-4]
    delete += ")"
    #print(delete)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(delete)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(delete)
        cnx.commit()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_delete_where:%s" % e)

def get_demand_hours():
    import pymssql
    global cnx, mssql_params

    # Este query obtiene la cantidad de viajes agrupados por hora de inicio (dtFechaInicio) 
    # y los ordena de forma ascendente por la hora.
    query = """
        SELECT DATEPART(HOUR, t.dtFechaInicio) AS Hour, COUNT(*) AS Demand
        FROM Viaje t
        GROUP BY DATEPART(HOUR, t.dtFechaInicio)
        ORDER BY Hour ASC
    """

    LOW_MAX = 2 
    PROMEDIO_MAX = 5
    BASTANTE_MAX = 10

    def demnadaTxT(count):
        if count <= LOW_MAX:
            return "Bajo"
        if count <= PROMEDIO_MAX:
            return "Promedio"
        if count <= BASTANTE_MAX:
            return "Bastante"
        return "Mucho"

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)

        results = cursor.fetchall()
        cursor.close()

        return [
            {
                "Hour": row["Hour"],
                "Demand": int(row["Demand"]),
                "Text": demnadaTxT(int(row["Demand"]))
            }
            for row in results
        ]

    except Exception as e:
        raise TypeError(f"get_demand_hours: {e}")

def get_operators_with_most_transfers():
    import pymssql
    global cnx, mssql_params
    # obtiene los operadores con más traslados realizados
    # y el total de traslados. Los resultados se agrupan por operador
    # y se ordenan en orden descendente según el número total de traslados.
    
    query = """
    SELECT u.IdUsuario, u.vcNombre, u.vcApellidoPaterno, u.vcApellidoMaterno, COUNT(t.IdTraslado) AS TotalTransfers
    FROM Usuarios u
    INNER JOIN Traslado t ON u.IdUsuario = t.IdUsuarioOperador
    GROUP BY u.IdUsuario, u.vcNombre, u.vcApellidoPaterno, u.vcApellidoMaterno
    ORDER BY TotalTransfers DESC
    """
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return [
            {
                "IdUsuario": row['IdUsuario'],
                "Nombre": " ".join(filter(None, [row.get('vcNombre'), row.get('vcApellidoPaterno'), row.get('vcApellidoMaterno')])),
                # "ApellidoPaterno": row.get('vcApellidoPaterno'),
                # "ApellidoMaterno": row.get('vcApellidoMaterno'),
                "TotalTransfers": row['TotalTransfers']
            }
            for row in results
        ]
    except Exception as e:
        raise TypeError(f"get_operators_with_most_transfers: {e}")

def get_monthly_transfer_percentages():
    import pymssql
    global cnx, mssql_params
    
    # calcula el porcentaje de traslados completados y cancelados por mes y año.
    # Agrupa los datos por año y mes de la fecha de inicio del viaje
    # Utiliza la tabla Traslado para obtener los estatus de los viajes y calcula los porcentajes
    # basándose en el total de registros por mes.
    # Los resultados se ordenan por año y mes.
    query = """
        SELECT 
            YEAR(v.dtFechaInicio) AS Year,
            MONTH(v.dtFechaInicio) AS Month,
            SUM(CASE WHEN e.vcEstatus = 'Terminado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS CompletedPercentage,
            SUM(CASE WHEN e.vcEstatus = 'Cancelado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS CanceledPercentage
        FROM Traslado tr
        INNER JOIN Estatus e ON tr.IdEstatus = e.IdEstatus
        INNER JOIN Viaje v ON tr.IdTraslado = v.IdTraslado
        GROUP BY YEAR(v.dtFechaInicio), MONTH(v.dtFechaInicio)
        ORDER BY Year, Month
    """

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)

        rows = cursor.fetchall()
        cursor.close()

        return [
            {
                "Year": row["Year"],
                "Month": row["Month"],
                "CompletedPercentage": float(row["CompletedPercentage"]) if row["CompletedPercentage"] is not None else 0.0,
                "CanceledPercentage": float(row["CanceledPercentage"]) if row["CanceledPercentage"] is not None else 0.0,
            }
            for row in rows
        ]

    except Exception as e:
        raise TypeError(f"get_monthly_transfer_percentages: {e}")

def get_maintenance():
    import pymssql
    from datetime import datetime
    global cnx, mssql_params
    
    # Este query obtiene información sobre las ambulancias:
    # - Su ID .
    # - La fecha de su próximo mantenimiento .
    # - El kilometraje más reciente registrado , obtenido del último viaje
    #   que tenga un valor de kilometraje final  no nulo, ordenado por la fecha de finalización del viaje.
    query = """
        SELECT 
            a.IdAmbulancia,
            a.dProxMantenimiento,
            (
                SELECT TOP (1) v.fKmFinal
                FROM Viaje v
                JOIN Traslado t ON t.IdTraslado = v.IdTraslado
                WHERE v.IdAmbulancia = a.IdAmbulancia
                  AND v.fKmFinal IS NOT NULL
                ORDER BY v.dtFechaFin DESC
            ) AS KmActual
        FROM Ambulancia a;
    """

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)

        rows = cursor.fetchall()
        cursor.close()

        today = datetime.today()

        def dias_para_mantenimiento(dt):
            if dt is None:
                return "Sin fecha"
            dias = (dt - today).days
            if dias <= 0:
                return "Urgente"
            return dias

        return [
            {
                "IdAmbu": f"Ambulancia {row['IdAmbulancia']}",
                "dMant": f"{dias_para_mantenimiento(row['dProxMantenimiento'])} dias",
                "KmActual": row["KmActual"] if row["KmActual"] is not None else 0
            }
            for row in rows
        ]

    except Exception as e:
        raise TypeError(f"get_maintenance: {e}")

def get_weekly_transfer_status():
    import pymssql
    global cnx, mssql_params
    #Stats de los viajes, obtiene los dias, cuenta cuanto hay de cada, y si hay algo que no es terminado y cancelado lo mete en pendiente
    #Pasa el numero de dia del 1 al 7. De la tabla traslado junta para saber el nombre de los estatus
    # Se unen los traslados registrados en viaje, pq un traslado puede tener uno o varios
    #filtra para solo la ultima semana, agrupa dia de la semana y nombre dia. ordena por dia de la semana
    query = """
    SELECT
        DATEPART(WEEKDAY, t.dtFechaInicio) AS DayIndex,
        DATENAME(WEEKDAY, t.dtFechaInicio) AS DayOfWeek,
        SUM(CASE WHEN e.vcEstatus = 'Terminado' THEN 1 ELSE 0 END) AS Completed,
        SUM(CASE WHEN e.vcEstatus = 'Cancelado' THEN 1 ELSE 0 END) AS Canceled,
        COUNT(*) - SUM(CASE WHEN e.vcEstatus IN ('Terminado','Cancelado') THEN 1 ELSE 0 END) AS Pending
    FROM Traslado tr
    INNER JOIN Estatus e ON tr.IdEstatus = e.IdEstatus
    INNER JOIN Viaje t ON tr.IdTraslado = t.IdTraslado
    WHERE t.dtFechaInicio >= DATEADD(DAY, -7, GETDATE())
    GROUP BY DATEPART(WEEKDAY, t.dtFechaInicio), DATENAME(WEEKDAY, t.dtFechaInicio)
    ORDER BY DayIndex
    """
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return [
            {
                "DayOfWeek": row["DayOfWeek"],
                "Completed": int(row["Completed"]),
                "Canceled": int(row["Canceled"]),
                "Pending": int(row["Pending"])
            }
            for row in rows
        ]
    except Exception as e:
        raise TypeError(f"get_weekly_transfer_status: {e}")
    
def get_ambulancias_disponibles(fecha_inicio, fecha_fin):
    import pymssql
    global cnx, mssql_params
    query = """
        SELECT * FROM Ambulancia 
        WHERE IdAmbulancia NOT IN (
            SELECT IdAmbulancia FROM Viaje 
            WHERE (dtFechaInicio < '%s' AND dtFechaFin > '%s')
        )
    """ % (fecha_fin, fecha_inicio)
    
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        raise TypeError("get_ambulancias_disponibles: %s" % e)


def get_tipo_ambulancia_por_id(idAmbulancia):
    import pymssql
    global cnx, mssql_params
    query = """
        SELECT ta.* 
        FROM tipoAmbulancia ta
        INNER JOIN Ambulancia a ON ta.IdTipoAmb = a.IdTipoAmb
        WHERE a.IdAmbulancia = %d
    """ % idAmbulancia
    
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as e:
        raise TypeError("get_tipo_ambulancia_por_id: %s" % e)


def get_proximo_numero_solicitud():
    import pymssql
    global cnx, mssql_params
    query = "SELECT MAX(IdTraslado) as ultimoId FROM Traslado"
    
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
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
    import pymssql
    global cnx, mssql_params
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


def get_traslados(params):
    import pymssql
    global cnx, mssql_params

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
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, tuple(values))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        raise TypeError("get_traslados:%s" % e)

