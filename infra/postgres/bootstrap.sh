#!/usr/bin/env bash
set -euo pipefail

export PGHOST="${PGHOST:-postgres}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:?PGPASSWORD is required}"

run() { psql -v ON_ERROR_STOP=1 -d postgres -tAc "$1"; }

ensure_user() {
  local user="$1" pass="$2"
  if [[ "$(run "SELECT 1 FROM pg_roles WHERE rolname='${user}'")" != "1" ]]; then
    run "CREATE USER ${user} WITH PASSWORD '${pass}'"
  else
    run "ALTER USER ${user} WITH PASSWORD '${pass}'"
  fi
}

ensure_db() {
  local db="$1" owner="$2"
  if [[ "$(run "SELECT 1 FROM pg_database WHERE datname='${db}'")" != "1" ]]; then
    run "CREATE DATABASE ${db} OWNER ${owner}"
  fi
}

grant_schema() {
  local db="$1" user="$2"
  psql -v ON_ERROR_STOP=1 -d "${db}" -c "GRANT ALL ON SCHEMA public TO ${user}"
}

ensure_user airflow airflow
ensure_user hive    hive

ensure_db airflow_metadata airflow
ensure_db nasa_db_raw      airflow
ensure_db datamart         airflow
ensure_db hive_metastore   hive

grant_schema airflow_metadata airflow
grant_schema nasa_db_raw      airflow
grant_schema datamart         airflow
grant_schema hive_metastore   hive

echo "postgres-bootstrap: users and databases ensured."
