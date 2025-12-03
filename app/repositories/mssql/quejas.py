from ...extensions import db
from app.repositories.s3.recursosS3 import getPresignedUrl
import pymssql

from ...extensions import db
from app.repositories.s3.recursosS3 import getPresignedUrl
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
        q.vcFoto,

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
        
        quejas = []
        for row in results:
            print(f"vcFoto value: {repr(row['vcFoto'])}")
            print(f"vcFoto type: {type(row['vcFoto'])}")
            
            queja = {
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
                "IdAmbulancia": row['IdAmbulancia'],
                
                # Foto
                "FotoKey": row['vcFoto']
            }
            
            # Solo generar URL si vcFoto tiene un valor válido
            foto_value = row.get('vcFoto')
            print(f"Evaluando foto: {repr(foto_value)}")
            
            if foto_value is not None and str(foto_value).strip():
                print(f"Intentando generar URL para: {foto_value}")
                try:
                    queja["FotoUrl"] = getPresignedUrl(str(foto_value))
                    print(f"URL generada: {queja['FotoUrl']}")
                except Exception as e:
                    print(f"Error generando URL para foto {foto_value}: {e}")
                    import traceback
                    traceback.print_exc()
                    queja["FotoUrl"] = None
            else:
                print("No se generará URL - foto vacía o None")
                queja["FotoUrl"] = None
            
            quejas.append(queja)
        
        return quejas
        
    except Exception as e:
        raise TypeError(f"getQuejas: {e}")
    
        
def updateQuejaEstado(id_queja):
    query = """
    UPDATE nova.dbo.Queja
    SET Estado = 1
    WHERE IdQueja = %s
    """

    try:
        cnx = db.get_mssql_connection()
        cursor = cnx.cursor()
        cursor.execute(query, (id_queja,))
        cnx.commit()
    except pymssql._pymssql.InterfaceError:
        print("Reconnecting...")
        cnx = db.reconnect()
        cursor = cnx.cursor()
        cursor.execute(query, (id_queja,))
        cnx.commit()
    finally:
        cursor.close()
        cnx.close()



def get_proximo_numero_queja():
    query = "SELECT MAX(IdQueja) as ultimoId FROM Queja"
    
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
        
        ultimo_id = result['ultimoId'] if result['ultimoId'] is not None else 0
        return ultimo_id + 1
    except Exception as e:
        raise TypeError("get_proximo_numero_queja: %s" % e)

def crear_queja(data):
    query = """
        INSERT INTO Queja (IdUsuarioOperador, vcTitulo, vcDetalles, IdPrioridad, vcFoto, Estado, dtFechaQueja, IdAmbulancia)
        VALUES (%d, '%s', '%s', %d, %s, 0, GETDATE(), %d);
        SELECT SCOPE_IDENTITY() as IdQueja;
    """ % (
        data['IdUsuarioOperador'],
        data['vcTitulo'].replace("'", "''"),
        data['vcDetalles'].replace("'", "''"),
        data['IdPrioridad'],
        "'%s'" % data['vcFoto'] if data.get('vcFoto') else 'NULL',
        data['IdAmbulancia']
    )
    
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
        cnx.commit()
        cursor.close()
        
        return result['IdQueja'] if result else None
    except Exception as e:
        raise TypeError("crear_queja: %s" % e)