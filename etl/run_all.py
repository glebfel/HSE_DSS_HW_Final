import argparse
import os
import glob
from typing import List

from etl.db import get_connection
from etl.stage_load import load_stage_from_csv


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
SQL_DIR = os.path.join(ROOT_DIR, "sql")


def _sql_paths(patterns: List[str]) -> List[str]:
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(SQL_DIR, pat)))
    files = sorted(set(files))
    return files


def run_sql_files_in_order(conn, paths: List[str]) -> None:
    """Выполнение SQL файлов в одном подключении"""
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def main(csv_path: str):
    with get_connection() as conn:
        # До загрузки: создаем схемы/таблицы/ограничения (00..40) — используем новые имена файлов
        pre_files = [
            os.path.join(SQL_DIR, "00_init_schemas.sql"),
            os.path.join(SQL_DIR, "01_staging_sample_superstore.sql"),
            os.path.join(SQL_DIR, "10_dv_hubs.sql"),
            os.path.join(SQL_DIR, "15_alias_views.sql"),
            os.path.join(SQL_DIR, "20_dv_links_and_satellites.sql"),
            os.path.join(SQL_DIR, "40_dv_constraints_and_indexes.sql"),
        ]
        run_sql_files_in_order(conn, pre_files)

    staged = load_stage_from_csv(csv_path)
    print({"stage_rows": staged})

    with get_connection() as conn:
        # После загрузки: ETL и проверки (50..60) — используем новые имена файлов
        post_files = [
            os.path.join(SQL_DIR, "50_etl_load_from_stage.sql"),
            os.path.join(SQL_DIR, "60_verify_metrics.sql"),
        ]
        run_sql_files_in_order(conn, post_files)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="End-to-end run: create DV, load stage, ETL, verify")
    parser.add_argument("--csv", required=True, help="Path to SampleSuperstore.csv")
    args = parser.parse_args()
    main(args.csv)