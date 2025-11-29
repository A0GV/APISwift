from ..extensions import db
import pymssql

def getQuejas():
    query = """
    SELECT 
        q.IdQueja,
        q.vcTitulo,
        q.vcDetalles,
        q.dtFechaQueja,
        q.dtFechaResolucion,
        q.Estado,

        -- Datos del Operador
        u.IdUsuario,
        u.vcNombre AS NombreOperador,
        u.vcApellidoPaterno AS APaternoOperador,
        u.vcApellidoMaterno AS AMaternoOperador,

        -- Prioridad
        p.IdPrioridad,
        p.vcPrioridad,

        -- Ambulancia (solo ID)
        a.IdAmbulancia
        
    FROM nova.dbo.Queja q
    INNER JOIN nova.dbo.Usuarios u
        ON q.IdUsuarioOperador = u.IdUsuario
    INNER JOIN nova.dbo.Prioridad p
        ON q.IdPrioridad = p.IdPrioridad
    LEFT JOIN nova.dbo.Ambulancia a
        ON q.IdAmbulancia = a.IdAmbulancia
    WHERE q.Estado = 0;   -- solo no resueltas
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
                "IdQueja": row['IdQueja'],
                "Titulo": row['vcTitulo'],
                "Detalles": row['vcDetalles'],
                "FechaQueja": row['dtFechaQueja'],
                "FechaResolucion": row['dtFechaResolucion'],
                "Estado": row['Estado'],
                # Datos del Operador
                "IdUsuario": row['IdUsuario'],
                "NombreOperador": " ".join(filter(None, [row.get('NombreOperador'), row.get('APaternoOperador'), row.get('AMaternoOperador')])),

                # Prioridad
                "IdPrioridad": row['IdPrioridad'],
                "Prioridad": row['vcPrioridad'],

                # Ambulancia
                "IdAmbulancia": row['IdAmbulancia']
            }
            for row in results
        ]
    except Exception as e:
        raise TypeError(f"getQuejas: {e}")

