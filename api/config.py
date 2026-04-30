import os

DATAMART_DSN = os.environ["DATAMART_DSN"]
JWT_SECRET   = os.environ["JWT_SECRET"]

USERS = {
    "admin":  os.environ.get("API_ADMIN_PASSWORD",  "admin"),
    "viewer": os.environ.get("API_VIEWER_PASSWORD", "viewer"),
}
