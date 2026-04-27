#!/bin/bash
set -e

PASSWORD_FILE="${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated"

if [ ! -f "$PASSWORD_FILE" ]; then
    cat > "$PASSWORD_FILE" <<EOF
{"admin": "${AIRFLOW_ADMIN_PASSWORD}", "invit": "${AIRFLOW_INVIT_PASSWORD}"}
EOF
fi

exec airflow standalone
