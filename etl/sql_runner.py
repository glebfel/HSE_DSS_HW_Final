import argparse

from etl.run_sql import run_sql_files_in_order


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL files against Greenplum/PostgreSQL (new module name)")
    parser.add_argument("files", nargs="*", help="Paths to SQL files; if empty, uses default order (new names)")
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
