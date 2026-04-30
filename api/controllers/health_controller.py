from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.get("/")
def root():
    return jsonify({
        "name": "Hubble Lakehouse API",
        "endpoints": [
            "GET  /health",
            "POST /auth/login",
            "POST /auth/refresh",
            "GET  /datamarts/biologiste",
            "GET  /datamarts/chimiste",
            "GET  /datamarts/ingenieur",
            "GET  /datamarts/physicien",
        ],
    })


@health_bp.get("/health")
def health():
    return jsonify({"status": "ok"})
