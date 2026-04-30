from flask import Blueprint, jsonify, request
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    get_jwt_identity,
    jwt_required,
)

from config import USERS

auth_bp = Blueprint("auth", __name__)


@auth_bp.post("/login")
def login():
    data = request.get_json(silent=True) or {}
    username = data.get("username")
    password = data.get("password")
    if not username or USERS.get(username) != password:
        return jsonify({"error": "invalid credentials"}), 401
    return jsonify({
        "access_token":  create_access_token(identity=username),
        "refresh_token": create_refresh_token(identity=username),
    })


@auth_bp.post("/refresh")
@jwt_required(refresh=True)
def refresh():
    return jsonify({
        "access_token": create_access_token(identity=get_jwt_identity()),
    })
