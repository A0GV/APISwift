from ..extensions import db
from datetime import datetime

def obtener_notificaciones_operador(id_usuario_operador, limit):
    # Mapeo manual de meses en español (más confiable que locale)
    MESES_ES = {
        1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril',
        5: 'mayo', 6: 'junio', 7: 'julio', 8: 'agosto',
        9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'
    }
    
    try:
        # Query para obtener traslados con su información de viaje y ubicación
        query = """
            SELECT 
                t.IdTraslado,
                t.dtFechaCreacion,
                v.dtFechaInicio,
                u.vcDomicilio,
                t.IdEstatus
            FROM Traslado t
            INNER JOIN Viaje v ON t.IdTraslado = v.IdTraslado
            INNER JOIN Ubicacion u ON t.IdUbiDest = u.IdUbicacion
            WHERE t.IdUsuarioOperador = %s
                AND t.IdEstatus IN (1, 2)
            ORDER BY t.dtFechaCreacion DESC
        """
        
        # Ejecutar query
        cnx = db.get_mssql_connection()
        cursor = cnx.cursor(as_dict=True)
        cursor.execute(query, (id_usuario_operador,))
        rows = cursor.fetchall()
        cursor.close()
        
        if not rows:
            return []
        
        # Procesar cada notificación
        notificaciones = []
        ahora = datetime.now()
        
        for row in rows:
            # Convertir strings a datetime si es necesario
            dt_creacion = row['dtFechaCreacion']
            dt_inicio = row['dtFechaInicio']
            
            if isinstance(dt_creacion, str):
                dt_creacion = datetime.fromisoformat(dt_creacion)
            if isinstance(dt_inicio, str):
                dt_inicio = datetime.fromisoformat(dt_inicio)
            
            # Calcular tiempo hasta el inicio
            tiempo_hasta_inicio = (dt_inicio - ahora).total_seconds()
            
            # Determinar msj, prioridad y trasladoAgendado
            if 0 < tiempo_hasta_inicio <= 3600:  # 1 hora en segundos
                msj = "Traslado por comenzar"
                prioridad = 1
                traslado_agendado = False
            elif dt_creacion.date() == ahora.date():
                msj = "Nuevo traslado"
                prioridad = 2
                traslado_agendado = True
            else:
                msj = "Traslado"
                prioridad = 3
                traslado_agendado = True
            
            # Determinar formato de hora
            if dt_creacion.date() == ahora.date():
                hora = dt_creacion.strftime("%I:%M %p")
            else:
                hora = dt_creacion.strftime("%d/%m")
            
            # Formatear detalles (usando mapeo manual de meses)
            domicilio_corto = row['vcDomicilio'].split(',')[0]
            mes_nombre = MESES_ES[dt_inicio.month]
            hora_inicio = dt_inicio.strftime("%I:%M %p")
            detalles = f"Hacia {domicilio_corto}, el día {dt_inicio.day} de {mes_nombre}, {hora_inicio}"
            
            # Crear objeto de notificación
            notificacion = {
                'IdTraslado': row['IdTraslado'],
                'msj': msj,
                'detalles': detalles,
                'hora': hora,
                'trasladoAgendado': traslado_agendado,
                'prioridad': prioridad,
                'dtFechaInicio': dt_inicio.isoformat(),
                'dtFechaCreacion': dt_creacion.isoformat()
            }
            
            notificaciones.append(notificacion)
        
        # Ordenar por prioridad (ascendente) y luego por fecha de creación (descendente)
        notificaciones_ordenadas = sorted(
            notificaciones,
            key=lambda x: (x['prioridad'], -datetime.fromisoformat(x['dtFechaCreacion']).timestamp())
        )
        
        if limit < len(notificaciones_ordenadas):
            return notificaciones_ordenadas[:limit]
        return notificaciones_ordenadas
        
    except Exception as e:
        raise TypeError("obtener_notificaciones_operador: %s" % e)

# Notifs de coordinador
def obtener_notificaciones_coordi(limit):
    # Mapeo manual de meses en español (más confiable que locale)
    MESES_ES = {
        1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril',
        5: 'mayo', 6: 'junio', 7: 'julio', 8: 'agosto',
        9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'
    }
    
    try:
        # Query para obtener traslados con su información de viaje y ubicación
        query = """
            SELECT TOP (%s)
                t.IdTraslado,
                t.IdUsuarioOperador,
                t.dtFechaCreacion,
                v.dtFechaInicio,
                uo.vcDomicilio AS OrigenDomicilio, 
                ud.vcDomicilio AS DestinoDomicilio,
                t.IdEstatus,
                -- Info del operador asignado
                op.vcNombre AS nombreOperador,
                op.vcApellidoPaterno AS apellidoOperador
            FROM Traslado t
            INNER JOIN Viaje v ON t.IdTraslado = v.IdTraslado
            JOIN dbo.Ubicacion uo ON t.IdUbiOrigen = uo.IdUbicacion
            JOIN dbo.Ubicacion ud ON t.IdUbiDest = ud.IdUbicacion
            LEFT JOIN Usuarios op ON t.IdUsuarioOperador = op.IdUsuario
            WHERE dtFechaCreacion IS NOT NULL
            ORDER BY t.dtFechaCreacion DESC;
        """
        
        # Ejecutar query
        cnx = db.get_mssql_connection()
        cursor = cnx.cursor(as_dict=True)
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        cursor.close()
        
        if not rows:
            return []
        
        # Procesar cada notificación
        notificaciones = []
        ahora = datetime.now()
        
        for row in rows:
            # Convertir strings a datetime si es necesario
            dt_creacion = row['dtFechaCreacion']
            dt_inicio = row['dtFechaInicio']
            
            if isinstance(dt_creacion, str):
                dt_creacion = datetime.fromisoformat(dt_creacion)
            if isinstance(dt_inicio, str):
                dt_inicio = datetime.fromisoformat(dt_inicio)
            
            # Calcular tiempo hasta el inicio
            tiempo_hasta_inicio = (dt_inicio - ahora).total_seconds()
            
            # Nombre del operador
            nombre_operador = f"{row.get('nombreOperador', '')} {row.get('apellidoOperador', '')}".strip()
            if not nombre_operador:
                nombre_operador = "Sin asignar"
            
            # Determinar msj, prioridad y trasladoAgendado
            if 0 < tiempo_hasta_inicio <= 3600:  # 1 hora en segundos
                msj = "Traslado por comenzar"
                prioridad = 1
                traslado_agendado = False
            elif dt_creacion.date() == ahora.date():
                msj = "Nuevo traslado"
                prioridad = 2
                traslado_agendado = True
            else:
                msj = "Traslado"
                prioridad = 3
                traslado_agendado = True
            
            # Determinar formato de hora
            if dt_creacion.date() == ahora.date():
                hora = dt_creacion.strftime("%I:%M %p")
            else:
                hora = dt_creacion.strftime("%d/%m")
            
            # Formatear detalles usando mapeo manual de meses y tmb info de operador
            domicilio_og = row['OrigenDomicilio'].split(',')[0]
            domicilio_dest = row['DestinoDomicilio'].split(',')[0]
            mes_nombre = MESES_ES[dt_inicio.month]
            hora_inicio = dt_inicio.strftime("%I:%M %p")
            detalles = f"De {domicilio_og} hacia {domicilio_dest}, el día {dt_inicio.day} de {mes_nombre}, {hora_inicio}"
            
            # Crear objeto de notificación
            notificacion = {
                'IdTraslado': row['IdTraslado'],
                'msj': msj,
                'detalles': detalles,
                'hora': hora,
                'trasladoAgendado': traslado_agendado,
                'prioridad': prioridad,
                'dtFechaInicio': dt_inicio.isoformat(),
                'dtFechaCreacion': dt_creacion.isoformat()
            }
            
            notificaciones.append(notificacion)
        
        # Ordenar por prioridad (ascendente) y luego por fecha de creación (descendente)
        notificaciones_ordenadas = sorted(
            notificaciones,
            key=lambda x: (x['prioridad'], -datetime.fromisoformat(x['dtFechaCreacion']).timestamp())
        )
        
        if limit < len(notificaciones_ordenadas):
            return notificaciones_ordenadas[:limit]
        return notificaciones_ordenadas
        
    except Exception as e:
        raise TypeError("obtener_notificaciones_coordinadores: %s" % e)