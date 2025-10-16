import argparse
import os
from typing import List

from etl.db import get_connection

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
SQL_DIR = os.path.join(ROOT_DIR, "sql")


def _abs_sql_path(p: str) -> str:
    return p if os.path.isabs(p) else os.path.join(SQL_DIR, p) if not p.startswith("sql/") else os.path.join(ROOT_DIR, p)


def run_sql_file(path: str) -> None:
    p = _abs_sql_path(path)
    if not os.path.exists(p):
        raise FileNotFoundError(p)
    sql = open(p, "r", encoding="utf-8").read()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def run_sql_files_in_order(paths: List[str]) -> None:
    with get_connection() as conn:
        for p in paths:
            p_abs = _abs_sql_path(p)
            if not os.path.exists(p_abs):
                raise FileNotFoundError(p_abs)
            with open(p_abs, "r", encoding="utf-8") as f:
                sql = f.read()
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL files against Greenplum")
    parser.add_argument("files", nargs="*", help="Paths to SQL files; if empty, uses default order")
    args = parser.parse_args()

    if args.files:
        run_sql_files_in_order(args.files)
    else:
        default_order = [
            "00_init_schemas.sql",
            "01_staging_sample_superstore.sql",
            "10_dv_hubs.sql",
            "15_alias_views.sql",
            "20_dv_links_and_satellites.sql",
            "40_dv_constraints_and_indexes.sql",
            "50_etl_load_from_stage.sql",
            "60_verify_metrics.sql",
        ]
        run_sql_files_in_order(default_order)
        print({"status": "ok", "ran_files": default_order})