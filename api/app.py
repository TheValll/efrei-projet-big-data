import datetime
import decimal
from datetime import timedelta

from flask import Flask, jsonify
from flask.json.provider import DefaultJSONProvider

import config
from controllers.auth_controller import auth_bp
from controllers.datamart_controller import datamart_bp
from controllers.health_controller import health_bp
from extensions import DBPool, jwt


class _JSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


def create_app() -> Flask:
    app = Flask(__name__)
    app.json = _JSONProvider(app)

    app.config["JWT_SECRET_KEY"]          = config.JWT_SECRET
    app.config["JWT_ACCESS_TOKEN_EXPIRES"]  = timedelta(hours=1)
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=7)

    jwt.init_app(app)
    DBPool.init(config.DATAMART_DSN)

    app.register_blueprint(health_bp)
    app.register_blueprint(auth_bp,     url_prefix="/auth")
    app.register_blueprint(datamart_bp, url_prefix="/datamarts")

    @app.errorhandler(404)
    def not_found(_):
        return jsonify({"error": "not found"}), 404

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
