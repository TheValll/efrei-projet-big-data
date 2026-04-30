from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required

from models import datamart_model

datamart_bp = Blueprint("datamarts", __name__)

DEFAULT_LIMIT = 100
MAX_LIMIT = 10000


def _pagination():
    try:
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        limit = DEFAULT_LIMIT
    try:
        offset = int(request.args.get("offset", 0))
    except ValueError:
        offset = 0
    return min(max(limit, 1), MAX_LIMIT), max(offset, 0)


@datamart_bp.get("/biologiste")
@jwt_required()
def biologiste():
    limit, offset = _pagination()
    rows = datamart_model.biologiste(
        from_date=request.args.get("from"),
        to_date=request.args.get("to"),
        limit=limit,
        offset=offset,
    )
    return jsonify({"count": len(rows), "limit": limit, "offset": offset, "rows": rows})


@datamart_bp.get("/chimiste")
@jwt_required()
def chimiste():
    limit, offset = _pagination()
    rows = datamart_model.chimiste(limit=limit, offset=offset)
    return jsonify({"count": len(rows), "limit": limit, "offset": offset, "rows": rows})


@datamart_bp.get("/ingenieur")
@jwt_required()
def ingenieur():
    limit, offset = _pagination()
    rows = datamart_model.ingenieur(
        from_date=request.args.get("from"),
        to_date=request.args.get("to"),
        limit=limit,
        offset=offset,
    )
    return jsonify({"count": len(rows), "limit": limit, "offset": offset, "rows": rows})


@datamart_bp.get("/physicien")
@jwt_required()
def physicien():
    limit, offset = _pagination()
    rows = datamart_model.physicien(limit=limit, offset=offset)
    return jsonify({"count": len(rows), "limit": limit, "offset": offset, "rows": rows})
