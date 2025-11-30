from flask import Blueprint, jsonify, request, make_response
from ..repositories.mssql.operador import get_user_data, post_user_config
from ..repositories.s3.recursosS3 import getPresignedUrl, postFile, deleteFile

operador_bp = Blueprint("operadores", __name__, url_prefix = "/api/operadores")

@operador_bp.route("/<int:idOperador>/datos", methods=['GET'])
def get_datos_operador(idOperador):
    try:
        operador_data = get_user_data(idOperador)  
        baseUrl = operador_data.pop('fotoUrlBase', None)
        if baseUrl:
            operador_data['fotoUrl'] = getPresignedUrl(baseUrl)
        else:
            operador_data['fotoUrl'] = None

        if operador_data:
            return make_response(jsonify(operador_data), 200)
        else:
            return make_response(jsonify({'error': 'Operador no encontrado'}), 404)
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
    
@operador_bp.route("/<int:idOperador>", methods=['POST'])
def update_datos_operador(idOperador):
    try:
        # Aquí iría la lógica para actualizar los datos del operador
        # Por ejemplo: update_user_data(idOperador, data)
        apodo = request.form.get('apodo')
        foto = request.files.get('foto')

        if foto: 
            print(foto.filename)
            deleteFile()
            url = postFile(foto, 'fotos-perfil/' + foto.filename)
            print(url)
            post_user_config(idOperador, apodo, url)
        else: 
            post_user_config(idOperador, apodo, None)
        return make_response(jsonify({'mensaje':'Se actualizó el perfil'}))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)
