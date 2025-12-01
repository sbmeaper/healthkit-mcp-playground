from pathlib import Path


# Where we expect the Apple Health export.xml to live
RAW_XML_PATH = Path(__file__).parent / "data" / "raw" / "export.xml"

# Where we will write the unified Parquet file
PARQUET_PATH = Path(__file__).parent / "data" / "processed" / "healthkit_records.parquet"


def main() -> None:
    print(f"Expecting HealthKit export at: {RAW_XML_PATH}")
    print(f"Will write Parquet file to: {PARQUET_PATH}")
    print("Ingestion logic not implemented yet.")


if __name__ == "__main__":
    main()