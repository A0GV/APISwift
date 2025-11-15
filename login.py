
# ========== ENDPOINTS DE LOGIN ==========
@app.route("/api/login/operador", methods=['POST'])
def login_operador_post():
    data = request.json
    id_usuario = data.get('idUsuario', None)
    vc_codigo = data.get('codigo', None)
    
    if not id_usuario or not vc_codigo:
        return make_response(jsonify({'error': 'Se requieren el id del Usuario y su contraseña'}), 400)
    try:
        # Primero buscar el usuario
        usuario = MSSql.sql_read_where('Usuarios', {'IdUsuario': id_usuario, 'IdTipoPersonal': 1})
        if not usuario:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
        
        # Obtener el IdPass del usuario
        id_pass = usuario[0]['IdPass']
        # Verificar la contraseña
        password_record = MSSql.sql_read_where('Pass', {'IdPass': id_pass, 'vcCodigoUsuario': vc_codigo})
        if password_record:
            return make_response(jsonify({'IdUsuario': int(id_usuario), 'message': 'Login exitoso'}), 200)
        else:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)

    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# Login para COORDINADORES (IdTipoPersonal = 2)
@app.route("/api/login/coordinador", methods=['POST'])
def login_coordinador():
    
    data = request.json
    
    id_usuario = data.get('idUsuario', None)
    vc_codigo = data.get('codigo', None)
    
    if not id_usuario or not vc_codigo:
        return make_response(jsonify({'error': 'Se requieren idUsuario y vcCodigoUsuario'}), 400)
    try:
        # Primero buscar el usuario
        usuario = MSSql.sql_read_where('Usuarios', {'IdUsuario': id_usuario, 'IdTipoPersonal': 2})
        
        if not usuario:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
        # Obtener el IdPass del usuario
        id_pass = usuario[0]['IdPass']
        
        # Verificar la contraseña
        password_record = MSSql.sql_read_where('Pass', {'IdPass': id_pass, 'vcCodigoUsuario': vc_codigo})
        if password_record:
            return make_response(jsonify({'IdUsuario': int(id_usuario), 'message': 'Login exitoso'}), 200)
        else:
            return make_response(jsonify({'error': 'Credenciales inválidas o permisos insuficientes'}), 401)
            
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
