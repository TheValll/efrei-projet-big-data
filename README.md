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
make up    # build and start the full stack
make down  # stop everything
make api   # tail the API logs
```

Once up:

- Airflow UI — <http://localhost:8080>
- API — <http://localhost:5000>
- HDFS NameNode UI — <http://localhost:9870>
- Spark master UI — <http://localhost:8081>

## Report

`docs/big_data_framework-valentin_massonniere-kevin_heugas_rapport.pdf`.
