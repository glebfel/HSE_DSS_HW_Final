import argparse

from etl.stage_load import load_stage_from_csv


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load SampleSuperstore.csv into stage.sample_superstore (new module name)")
    parser.add_argument("--csv", required=True, help="Path to SampleSuperstore.csv")
    args = parser.parse_args()
    total = load_stage_from_csv(args.csv)
    print({"stage_rows": total})
