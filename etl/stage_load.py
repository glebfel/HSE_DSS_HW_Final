import argparse
import os
from etl.db import get_connection


def _env_bool(primary: str, fallback: str | None = None, default_true: bool = True) -> bool:
    values = []
    if primary:
        values.append(os.environ.get(primary))
    if fallback:
        values.append(os.environ.get(fallback))
    for v in values:
        if v is not None:
            return str(v).lower() not in ("0", "false", "no")
    return default_true


def load_stage_from_csv(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    # Support new ETL_TRUNCATE_STAGE with fallback to legacy TRUNCATE_STAGE
    truncate = _env_bool("ETL_TRUNCATE_STAGE", fallback="TRUNCATE_STAGE", default_true=True)

    with get_connection() as conn:
        with conn.cursor() as cur:
            if truncate:
                cur.execute("TRUNCATE TABLE stage.sample_superstore")

            copy_sql = (
                "COPY stage.sample_superstore (\n"
                "  ship_mode, segment, country, city, state, postal_code, region,\n"
                "  category, sub_category, sales, quantity, discount, profit\n"
                ") FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            )

            with open(csv_path, "r", encoding="utf-8") as f:
                with cur.copy(copy_sql) as copy:
                    while data := f.read(8192):
                        copy.write(data)

        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM stage.sample_superstore")
            rowcount = cur.fetchone()[0]
            return int(rowcount)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load SampleSuperstore.csv into stage.sample_superstore")
    parser.add_argument("--csv", required=True, help="Path to SampleSuperstore.csv")
    args = parser.parse_args()
    total = load_stage_from_csv(args.csv)
    print({"stage_rows": total})