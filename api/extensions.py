from flask_jwt_extended import JWTManager
from psycopg2 import pool

jwt = JWTManager()


class DBPool:
    _pool = None

    @classmethod
    def init(cls, dsn: str, minconn: int = 1, maxconn: int = 5) -> None:
        cls._pool = pool.SimpleConnectionPool(minconn, maxconn, dsn)

    @classmethod
    def conn(cls):
        return cls._pool.getconn()

    @classmethod
    def release(cls, conn) -> None:
        cls._pool.putconn(conn)
