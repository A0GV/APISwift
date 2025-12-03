from flask import Blueprint, jsonify, request, make_response

from app.repositories.mssql.mysqlfunc import sql_read_all
from ..repositories.mssql.quejas import get_proximo_numero_queja, getQuejas, updateQuejaEstado, crear_queja
from ..repositories.s3.recursosS3 import postFile
from flask_jwt_extended import jwt_required
from ..models.roles import role_required
from app.extensions import limiter

quejas_bp = Blueprint("quejas", __name__, url_prefix="/api/quejas")

# GET quejas activas
@quejas_bp.route("/obtenerquejas", methods=["GET"])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def obtener_quejas():
	try:
		results = getQuejas()
		return make_response(jsonify(results), 200)
	except Exception as e:
		return make_response(jsonify({'error': str(e)}), 500)

# PUT endpoint to update the status of a complaint
@quejas_bp.route("/actualizarEstado", methods=["PUT"])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("coordinador")
def actualizar_estado():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"error": "JSON inválido"}), 400)
            
        id_queja = data.get("IdQueja")

        # Validación de tamaño y tipo
        if not id_queja:
            return make_response(jsonify({"error": "Se requiere IdQueja"}), 400)
        if not str(id_queja).isdigit() or len(str(id_queja)) > 10:
            return make_response(jsonify({"error": "IdQueja inválido"}), 400)

        updateQuejaEstado(id_queja)

        return make_response(jsonify({"message": "Estado actualizado correctamente."}), 200)

    except Exception as e:
        return make_response(jsonify({"error": "Error interno del servidor"}), 500)


# GET catálogos para quejas (ambulancias + próximo número)
@quejas_bp.route("/catalogos-queja", methods=['GET'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def get_catalogos_queja():
    try:
        ambulancias = sql_read_all('Ambulancia')
        proximo_numero = get_proximo_numero_queja()
        
        return make_response(jsonify({
            'ambulancias': ambulancias,
            'proximoNumero': proximo_numero
        }))
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)

# POST crear queja
@quejas_bp.route("", methods=['POST'])
@limiter.limit("5 per minute")
@jwt_required()
@role_required("operador")
def post_crear_queja():
    try:
        # Obtener datos del form
        id_operador = request.form.get('IdUsuarioOperador', type=int)
        titulo = request.form.get('vcTitulo')
        detalles = request.form.get('vcDetalles')
        id_prioridad = request.form.get('IdPrioridad', type=int)
        id_ambulancia = request.form.get('IdAmbulancia', type=int)
        foto = request.files.get('foto')
        
        # Validar campos requeridos
        if not all([id_operador, titulo, detalles, id_prioridad, id_ambulancia]):
            return make_response(jsonify({'error': 'Faltan campos requeridos'}), 400)
        
        # Subir foto a S3 si existe
        foto_url = None
        if foto:
            key = 'quejas/' + foto.filename
            if postFile(foto, key):
                foto_url = key
        
        # Crear queja en base de datos
        data = {
            'IdUsuarioOperador': id_operador,
            'vcTitulo': titulo,
            'vcDetalles': detalles,
            'IdPrioridad': id_prioridad,
            'IdAmbulancia': id_ambulancia,
            'vcFoto': foto_url
        }
        
        id_queja = crear_queja(data)
        
        if id_queja:
            return make_response(jsonify({
                'IdQueja': int(id_queja),
                'message': 'Queja registrada exitosamente'
            }), 201)
        else:
            return make_response(jsonify({'error': 'Error al crear la queja'}), 500)
            
    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 500)