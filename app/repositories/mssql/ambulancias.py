from ...extensions import db
import pymssql

def get_maintenance():
    from datetime import datetime

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
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
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


def sql_get_ambulancias_disponibles(fecha_inicio, fecha_fin, excluir_viaje=None):
    # Query con JOIN para incluir tipo de ambulancia
    query = """
        SELECT 
            a.IdAmbulancia,
            a.IdTipoAmb,
            a.dProxMantenimiento,
            ta.tipo as vcTipoAmbulancia
        FROM Ambulancia a
        INNER JOIN tipoAmbulancia ta ON a.IdTipoAmb = ta.IdTipoAmb
        WHERE a.IdAmbulancia NOT IN (
            SELECT v.IdAmbulancia FROM Viaje v
            WHERE v.dtFechaInicio < '%s' AND v.dtFechaFin > '%s'
    """ % (fecha_fin, fecha_inicio)
    
    # Excluir el viaje actual si se especifica
    if excluir_viaje:
        query += " AND v.IdViaje != %d" % excluir_viaje
    
    query += ")"
    
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
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        raise TypeError("get_ambulancias_disponibles: %s" % e)
    


# Obitiene km de viaje más reciente terminado de la ambulancia
def sql_read_last_amt_km(IdAmbulancia):    
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
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdAmbulancia,)) # Adds ambulance id 
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (IdAmbulancia,))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_last_amt_km:%s" % e)


def get_tipo_ambulancia_por_id(idAmbulancia):
    query = """
        SELECT ta.* 
        FROM tipoAmbulancia ta
        INNER JOIN Ambulancia a ON ta.IdTipoAmb = a.IdTipoAmb
        WHERE a.IdAmbulancia = %d
    """ % idAmbulancia
    
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
        raise TypeError("get_tipo_ambulancia_por_id: %s" % e)
