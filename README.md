# Hubble Lakehouse

Efrei --- Big Data Framework, by Valentin Massonnière and Kevin Heugas.

Medallion Lakehouse pipeline (Bronze, Silver, Gold) crossing the Hubble Space Telescope archive (MAST, ~500,000 rows) with NOAA SWPC space weather data, exposed through a Flask API protected by JWT.

## Endpoints

- Airflow: <https://efrei-bigdata-airflow.valentin-massonniere.ch>
- API: <https://efrei-bigdata-api.valentin-massonniere.ch>

## Data sources

The MAST CSV export is not versioned (size). Download it from [Google Drive](https://drive.google.com/file/d/173ggYT09-3R5hg955yev-OIPsnndhM4z/view?usp=sharing) and put it in `ingestion/`.

## Local setup

Copy `.env.example` to `.env`, then:

```bash
make up       # build and start the full stack
make down     # stop everything
make dump     # dump the db local
make restore  # restore a local dump
```

Once up:

- Airflow UI --- <http://localhost:8080>
- API --- <http://localhost:5000>
- HDFS NameNode UI --- <http://localhost:9870>
- Spark master UI --- <http://localhost:8081>
- Postgres datamart --- `postgresql://airflow:airflow@localhost:5432/datamart` (PowerBI: server `localhost:5432`, db `datamart`, user `airflow`, password `airflow`)

## API usage

The API is structured in MVC (`controllers/`, `models/`, `app.py`) and protected by JWT (access 1h, refresh 7d). Two users are provisioned via env vars: `admin` and `viewer`.

## Bruno collection

A ready-to-use [Bruno](https://www.usebruno.com/) collection lives in `bruno/` (open the folder in Bruno --- "Open collection"). It includes:

- two environments: `local` (<http://localhost:5000>) and `prod` (deployed VPS)
- `auth/Login` --- automatically stores `access_token` and `refresh_token` in env vars
- `auth/Refresh` --- uses the refresh token, refreshes `access_token`
- `datamarts/{Biologiste,Chimiste,Ingenieur,Physicien}` --- pre-filled query params, auth via `{{access_token}}`

## Report

`docs/big_data_framework-valentin_massonniere-kevin_heugas_rapport.pdf`.
