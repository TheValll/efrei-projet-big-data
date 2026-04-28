"""First-start loader: COPY the MAST CSV into public.mast if the table is empty.

The MAST export prefixes the file with comment lines starting with '#'
(human-readable column names + datatypes) before the real CSV header,
so we skip those before handing the stream to Postgres COPY.
"""

import os
import sys

import psycopg2

DSN = os.environ["NASA_DB_RAW_DSN"]
CSV_PATH = os.environ["MAST_CSV"]


def is_already_loaded(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM public.mast LIMIT 1")
        return cur.fetchone() is not None


def skip_comment_header(f) -> None:
    while True:
        pos = f.tell()
        line = f.readline()
        if not line:
            return
        if not line.startswith(b"#"):
            f.seek(pos)
            return


def main() -> int:
    if not os.path.isfile(CSV_PATH):
        print(f"MAST CSV not found at {CSV_PATH}, skipping load")
        return 0

    with psycopg2.connect(DSN) as conn:
        if is_already_loaded(conn):
            print("public.mast already populated, skipping load")
            return 0

        print(f"Loading {CSV_PATH} into public.mast")
        with open(CSV_PATH, "rb") as f:
            skip_comment_header(f)
            with conn.cursor() as cur:
                cur.copy_expert(
                    "COPY public.mast FROM STDIN WITH (FORMAT csv, HEADER true)",
                    f,
                )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.mast")
            (count,) = cur.fetchone()
        print(f"public.mast loaded ({count} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
