from contextlib import contextmanager

from psycopg2.extras import RealDictCursor

from extensions import DBPool


@contextmanager
def _cursor():
    conn = DBPool.conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    finally:
        DBPool.release(conn)


def biologiste(from_date, to_date, limit, offset):
    sql = """
        SELECT obs_id, t_start, t_end, flare_class, risk_score
        FROM public.biologiste
        WHERE TRUE
    """
    params = []
    if from_date:
        sql += " AND t_start >= %s"
        params.append(from_date)
    if to_date:
        sql += " AND t_start <= %s"
        params.append(to_date)
    sql += " ORDER BY t_start LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    with _cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def chimiste(limit, offset):
    sql = """
        SELECT target_name, filter, emission_line, nb_obs
        FROM public.chimiste
        ORDER BY nb_obs DESC
        LIMIT %s OFFSET %s
    """
    with _cursor() as cur:
        cur.execute(sql, (limit, offset))
        return cur.fetchall()


def ingenieur(from_date, to_date, limit, offset):
    sql = """
        SELECT instrument, year_month, avg_calib_time, trend_12m
        FROM public.ingenieur
        WHERE TRUE
    """
    params = []
    if from_date:
        sql += " AND year_month >= %s"
        params.append(from_date)
    if to_date:
        sql += " AND year_month <= %s"
        params.append(to_date)
    sql += " ORDER BY instrument, year_month LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    with _cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def physicien(limit, offset):
    sql = """
        SELECT ra_bin, dec_bin, nb_obs, total_exptime
        FROM public.physicien
        ORDER BY nb_obs DESC
        LIMIT %s OFFSET %s
    """
    with _cursor() as cur:
        cur.execute(sql, (limit, offset))
        return cur.fetchall()
