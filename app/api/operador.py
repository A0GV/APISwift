from flask import Blueprint, jsonify, request, make_response
from ..repositories.mssql.operador import get_user_data, post_user_config
from ..repositories.s3.recursosS3 import getPresignedUrl, postFile, deleteFile
from flask_jwt_extended import jwt_required
from ..models.roles import role_required

operador_bp = Blueprint("operadores", __name__, url_prefix = "/api/operadores")

@jwt_required()
@role_required("operador")
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
    
@jwt_required()
@role_required("operador")
@operador_bp.route("/<int:idOperador>", methods=['POST'])
def update_datos_operador(idOperador):
    try:
        apodo = request.form.get('apodo')
        foto = request.files.get('foto')
        modificarFoto = request.form.get('modificarFoto', 'false').lower() == 'true'

        url = ""
        deleteOutdatedUrl = False

        if modificarFoto:
            urlFotoPasada = get_user_data(idOperador).get('fotoUrlBase')
            if urlFotoPasada:
                deleteFile(urlFotoPasada)

            if foto:
                url = 'fotos-perfil/' + foto.filename
                postFile(foto, url)
            else: 
                url = None
                deleteOutdatedUrl = True
        else: 
            url = get_user_data(idOperador).get('fotoUrlBase')
        
        post_user_config(idOperador, apodo, url if modificarFoto else None, deleteOutdatedUrl)


        return make_response(jsonify({
            'mensaje':'Se actualizó el perfil',
            'usuario':{
                'idOperador': idOperador,
                'apodo': apodo,
                'fotoUrl': getPresignedUrl(url) if foto else None
            }
            }),200)
    except Exception as e:
        print(str(e))
        return make_response(jsonify({'error': str(e)}), 500)
