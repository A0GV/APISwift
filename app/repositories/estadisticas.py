from ..extensions import db
import pymssql

def get_demand_hours():

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
    


def get_weekly_transfer_status():
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
            cnx = db.get_mssql_connection()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute("SET LANGUAGE Spanish;")
            cursor.execute(query)
        except pymssql._pymssql.InterfaceError:
            cnx = db.reconnect()
            cursor = cnx.cursor(as_dict=True)
            cursor.execute("SET LANGUAGE Spanish;")
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
