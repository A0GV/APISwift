from flask import Blueprint, jsonify, request, make_response

viajes_bp = Blueprint("viajes", __name__, url_prefix="/api/viajes")
