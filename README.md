# Hubble Lakehouse

Efrei --- Big Data Framework, by Valentin Massonnière and Kevin Heugas.

Medallion Lakehouse pipeline (Bronze, Silver, Gold) that crosses the Hubble Space Telescope observation archive (MAST, ~500,000 rows) with NOAA SWPC space weather data, and feeds four PostgreSQL datamarts exposed through a Flask API protected by JWT.

## Endpoints

- Airflow: <https://efrei-bigdata-airflow.valentin-massonniere.ch> (invit/`inviter123`, read only)
- API: <https://efrei-bigdata-api.valentin-massonniere.ch>

## Data sources

The MAST CSV export is not versioned (size). Download it from [Google Drive](https://drive.google.com/file/d/173ggYT09-3R5hg955yev-OIPsnndhM4z/view?usp=sharing) and put it in `ingestion/`.

## Local setup

Copy `.env.example` to `.env` and fill in the values.

```bash
make install
make api
```

The Flask API then listens on <http://localhost:5000>.

## Structure

```
api/         Flask API (datamarts via JWT)
dags/        Airflow DAGs
infra/       Dockerfiles and Kubernetes manifests
docs/        LaTeX report
```

## Report

`docs/big_data_framework-valentin_massonniere-kevin_heugas_rapport.pdf`.
