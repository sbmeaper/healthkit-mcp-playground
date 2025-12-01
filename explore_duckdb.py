from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).parent
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "healthkit_records.parquet"


def main() -> None:
    print(f"Reading from Parquet: {PARQUET_PATH}")

    # Connect to DuckDB in memory
    con = duckdb.connect(database=":memory:")

    # Expose the Parquet file as a view named healthkit_records
    con.execute(f"""
        CREATE VIEW healthkit_records AS
        SELECT * FROM read_parquet('{PARQUET_PATH.as_posix()}');
    """)

    # Example: top 10 record types by row count
    df = con.execute("""
        SELECT
            type,
            unit,
            COUNT(*) AS n
        FROM healthkit_records
        GROUP BY type, unit
        ORDER BY n DESC
        LIMIT 10;
    """).df()

    print("\nTop 10 record types by row count:")
    print(df)

    con.close()


if __name__ == "__main__":
    main()