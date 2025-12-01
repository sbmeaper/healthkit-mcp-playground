from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).parent
XML_PATH = PROJECT_ROOT / "data" / "raw" / "export.xml"
OUTPUT_PARQUET = PROJECT_ROOT / "data" / "processed" / "healthkit_records.parquet"

def count_records(path: Path, limit: int = 1000) -> None:
    count = 0
    for event, elem in ET.iterparse(path, events=("start",)):
        if elem.tag == "Record":
            count += 1
            if count % 100 == 0:
                print(f"Seen {count} <Record> elements so far...")
            if count >= limit:
                break
    print(f"Finished. Counted {count} <Record> elements (up to limit={limit}).")

def records_to_parquet(xml_path: Path, parquet_path: Path, chunk_size: int = 50_000) -> None:
    """
    Stream all <Record> elements from the HealthKit XML into a Parquet file.

    - xml_path: path to export.xml
    - parquet_path: where to write the .parquet file
    - chunk_size: how many rows to buffer before writing a chunk
    """
    rows = []
    writer = None
    all_columns = None  # fixed set of columns for the Parquet schema

    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag == "Record":
            rows.append(elem.attrib)

            if len(rows) >= chunk_size:
                df = pd.DataFrame(rows)

                # On the first chunk, freeze the column set
                if all_columns is None:
                    all_columns = sorted(df.columns)

                # Reindex to the fixed column set so every chunk matches the schema
                df = df.reindex(columns=all_columns)

                # Force all columns to string dtype so schema is stable across chunks
                for col in df.columns:
                    df[col] = df[col].astype("string")

                table = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(str(parquet_path), table.schema)

                writer.write_table(table)
                rows.clear()

            # free memory for this element
            elem.clear()

    # write any remaining rows
    if rows:
        df = pd.DataFrame(rows)

        if all_columns is None:
            all_columns = sorted(df.columns)

        df = df.reindex(columns=all_columns)

        for col in df.columns:
            df[col] = df[col].astype("string")

        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(str(parquet_path), table.schema)

        writer.write_table(table)

    if writer is not None:
        writer.close()

    print(f"Finished writing Parquet to: {parquet_path}")

def main() -> None:
    print(f"Expecting HealthKit export at: {XML_PATH}")
    print(f"Will write Parquet file to: {OUTPUT_PARQUET}")

    # Quick sanity check on the XML
    count_records(XML_PATH, limit=500)

    # Full ingest to Parquet
    records_to_parquet(XML_PATH, OUTPUT_PARQUET, chunk_size=50_000)


if __name__ == "__main__":
    main()