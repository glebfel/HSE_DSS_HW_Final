import argparse

from etl.run_all import main as pipeline_main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="End-to-end pipeline: create DV, load stage, ETL, verify (new module name)")
    parser.add_argument("--csv", required=True, help="Path to SampleSuperstore.csv")
    args = parser.parse_args()
    pipeline_main(args.csv)
