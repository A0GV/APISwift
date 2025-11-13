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
    query = """
    SELECT DATEPART(HOUR, dtFechaInicio) AS Hour, COUNT(*) AS Demand
    FROM Viaje
    GROUP BY DATEPART(HOUR, dtFechaInicio)
    ORDER BY Demand DESC
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
        return [{"Hour": row['Hour'], "Demand": row['Demand']} for row in results]
    except Exception as e:
        raise TypeError("get_demand_hours: %s" % e)

def get_operators_with_most_transfers():
    import pymssql
    global cnx, mssql_params
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
                "Nombre": row['vcNombre'],
                "ApellidoPaterno": row['vcApellidoPaterno'],
                "ApellidoMaterno": row['vcApellidoMaterno'],
                "TotalTransfers": row['TotalTransfers']
            }
            for row in results
        ]
    except Exception as e:
        raise TypeError(f"get_operators_with_most_transfers: {e}")

def get_monthly_transfer_percentages():
    import pymssql
    global cnx, mssql_params
    query = """
    SELECT 
        MONTH(t.dtFechaInicio) AS Month, 
        YEAR(t.dtFechaInicio) AS Year,
        SUM(CASE WHEN e.vcEstatus = 'Terminado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS CompletedPercentage,
        SUM(CASE WHEN e.vcEstatus = 'Cancelado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS CanceledPercentage
    FROM Traslado tr
    INNER JOIN Estatus e ON tr.IdEstatus = e.IdEstatus
    INNER JOIN Viaje t ON tr.IdTraslado = t.IdTraslado
    GROUP BY YEAR(t.dtFechaInicio), MONTH(t.dtFechaInicio)
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
        results = cursor.fetchall()
        cursor.close()
        return [
            {
                "Year": row['Year'],
                "Month": row['Month'],
                "CompletedPercentage": row['CompletedPercentage'],
                "CanceledPercentage": row['CanceledPercentage']
            }
            for row in results
        ]
    except Exception as e:
        raise TypeError(f"get_monthly_transfer_percentages: {e}")

def get_weekly_transfer_status():
    import pymssql
    global cnx, mssql_params
    query = """
    SELECT 
        DATENAME(WEEKDAY, t.dtFechaInicio) AS DayOfWeek,
        e.vcEstatus AS Status,
        COUNT(*) AS Total
    FROM Traslado tr
    INNER JOIN Estatus e ON tr.IdEstatus = e.IdEstatus
    INNER JOIN Viaje t ON tr.IdTraslado = t.IdTraslado
    WHERE t.dtFechaInicio >= DATEADD(DAY, -7, GETDATE())
    GROUP BY DATENAME(WEEKDAY, t.dtFechaInicio), e.vcEstatus
    ORDER BY DayOfWeek, Total DESC
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
                "DayOfWeek": row['DayOfWeek'],
                "Status": row['Status'],
                "Total": row['Total']
            }
            for row in results
        ]
    except Exception as e:
        raise TypeError(f"get_weekly_transfer_status: {e}")

if __name__ == '__main__':
    import json
    mssql_params = {}
    mssql_params['DB_HOST'] = '100.80.80.7' #'10.14.255.41' 
    mssql_params['DB_NAME'] = 'noca'
    mssql_params['DB_USER'] = 'SA'
    mssql_params['DB_PASSWORD'] = 'Shakira123.'
    cnx = mssql_connect(mssql_params)
    # Do your thing
    try:
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
        rx = read_user_data('users', 'hugo')
        print(rx)
        input("press Enter to continue...")
        print("Querying for user 'paco'...")
        d_where = {'username': 'paco'}
        rx = sql_read_where('users', d_where)
        print(rx)
        input("press Enter to continue...")
        print("Inserting user 'otro'...")
        rx = sql_insert_row_into('users',{'username': 'otro', 'password': 'otro123'})
        print("Inserted record", rx)
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
        print("Modifying password for user 'otro'...")
        d_field = {'password': 'otro456'}
        d_where = {'username': 'otro'}
        sql_update_where('users', d_field, d_where)
        print("Record updated")
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
        print("Deleting user 'otro'...")
        d_where = {'username': 'otro'}
        sql_delete_where('users', d_where)
        print("Record deleted")
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
    except Exception as e:
        print(e)
    cnx.close()