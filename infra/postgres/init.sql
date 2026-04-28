CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_metadata OWNER airflow;
CREATE DATABASE nasa_db_raw OWNER airflow;
CREATE DATABASE datamart OWNER airflow;

CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE hive_metastore OWNER hive;
\c hive_metastore
GRANT ALL ON SCHEMA public TO hive;

\c airflow_metadata
GRANT ALL ON SCHEMA public TO airflow;

\c nasa_db_raw
GRANT ALL ON SCHEMA public TO airflow;

CREATE TABLE IF NOT EXISTS public.mast (
    dataproduct_type      TEXT,
    calib_level           TEXT,
    obs_collection        TEXT,
    obs_id                TEXT,
    target_name           TEXT,
    s_ra                  TEXT,
    s_dec                 TEXT,
    t_min                 TEXT,
    t_max                 TEXT,
    t_exptime             TEXT,
    wavelength_region     TEXT,
    filters               TEXT,
    em_min                TEXT,
    em_max                TEXT,
    target_classification TEXT,
    obs_title             TEXT,
    t_obs_release         TEXT,
    instrument_name       TEXT,
    proposal_pi           TEXT,
    proposal_id           TEXT,
    proposal_type         TEXT,
    project               TEXT,
    sequence_number       TEXT,
    provenance_name       TEXT,
    s_region              TEXT,
    "jpegURL"             TEXT,
    "dataURL"             TEXT,
    "dataRights"          TEXT,
    "mtFlag"              TEXT,
    "srcDen"              TEXT,
    "intentType"          TEXT,
    obsid                 TEXT,
    "objID"               TEXT,
    wave_min              TEXT,
    wave_max              TEXT,
    wave_region           TEXT
);

ALTER TABLE public.mast OWNER TO airflow;
