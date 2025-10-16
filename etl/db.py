import os
import psycopg
from typing import Optional, Iterable


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def get_env_any(names: Iterable[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = os.environ.get(n)
        if v not in (None, ""):
            return v
    return default


def get_connection():
    # Use DB_* env variables; allow PGHOST as single-host fallback
    hosts = get_env_any(["DB_HOSTS"])
    if not hosts:
        host_single = get_env_any(["DB_HOST", "PGHOST"])  # allow single-host fallback
        hosts = host_single
    if not hosts:
        raise RuntimeError("DB_HOSTS (or DB_HOST) is required (comma-separated hostnames)")

    port = int(get_env_any(["DB_PORT"], "6432"))
    dbname = get_env_any(["DB_NAME", "DB_DBNAME"], "postgres")
    user = get_env_any(["DB_USER"])
    password = get_env_any(["DB_PASSWORD"])
    if not user or not password:
        raise RuntimeError("DB_USER and DB_PASSWORD are required")

    target_session_attrs = get_env_any(["DB_TARGET_SESSION_ATTRS"], "read-write")
    sslmode = get_env_any(["DB_SSLMODE"], "verify-full")
    sslrootcert = os.path.expanduser(get_env_any(["DB_SSLROOTCERT"], "~/.postgresql/root.crt"))

    dsn = (
        f"host={hosts} port={port} dbname={dbname} user={user} password={password} "
        f"target_session_attrs={target_session_attrs} sslmode={sslmode} sslrootcert={sslrootcert}"
    )
    conn = psycopg.connect(dsn)
    return conn


if __name__ == "__main__":
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), version()")
            row = cur.fetchone()
            print({"database": row[0], "version": row[1].split(" on ")[0]})