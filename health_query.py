from pathlib import Path
from typing import Dict, Any, TypedDict, Literal  # <- add TypedDict, Literal

import yaml
import duckdb
import datetime
import pandas as pd




PROJECT_ROOT = Path(__file__).parent

METRICS_PATH = PROJECT_ROOT / "metrics.yaml"
DIMENSIONS_PATH = PROJECT_ROOT / "dimensions.yaml"
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "healthkit_records.parquet"

class HealthQuerySpec(TypedDict, total=False):
    """
    Contract for the HealthKit query MCP tool.

    Fields:
      - metric: logical metric name from metrics.yaml (e.g., "heart_rate").
      - aggregation: SQL aggregation to apply: "avg", "sum", "min", "max", "count".
      - time_grain: time bucket: "day", "week", "month".
      - limit: max number of rows to return.
      - time_range: optional {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"} filter on startDate.
    """
    metric: str
    aggregation: Literal["avg", "sum", "min", "max", "count"]
    time_grain: Literal["day", "week", "month"]
    limit: int
    time_range: Dict[str, str]

def load_yaml(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def get_time_group_expr(time_grain: str, dimensions_cfg: dict) -> tuple[str, str]:
    """
    Map a logical time_grain ('day', 'month', 'week') to a SQL expression
    using dimensions.yaml. Returns (expr, alias).
    """
    time_dims = dimensions_cfg["dimensions"]["time"]["fields"]

    if time_grain == "day":
        return time_dims["date"]["expr"], "date"
    elif time_grain == "month":
        return time_dims["month"]["expr"], "month"
    elif time_grain == "week":
        return time_dims["week"]["expr"], "week"
    else:
        # Fallback: full start timestamp
        return time_dims["start_timestamp"]["expr"], "start_timestamp"


def build_health_query_sql(
    spec: Dict[str, Any],
    metrics_cfg: dict,
    dimensions_cfg: dict,
) -> str:
    """
    Turn a simple spec like:
      {"metric": "heart_rate", "aggregation": "avg", "time_grain": "day", "limit": 30}
    into a SQL query over healthkit_records using the semantic layer.
    """
    metric_name = spec["metric"]
    metrics = metrics_cfg.get("metrics", {})
    metric_def = metrics.get(metric_name)
    if metric_def is None:
        raise ValueError(f"Unknown metric: {metric_name}")

    # Global config
    table = metrics_cfg.get("table", "healthkit_records")
    value_field = metrics_cfg.get("value_field", "value")
    defaults = metrics_cfg.get("defaults", {})

    # Aggregation + casting
    agg = spec.get("aggregation") or metric_def.get("default_agg") or defaults.get("aggregation", "avg")
    cast_type = metric_def.get("cast") or defaults.get("cast", "double")

    # Time grain
    time_grain = spec.get("time_grain") or defaults.get("time_grain", "day")
    group_expr, group_alias = get_time_group_expr(time_grain, dimensions_cfg)

    # Metric filters (hk_type + unit)
    hk_type = metric_def["hk_type"]
    unit = metric_def.get("unit")

    where_clauses = [f"type = '{hk_type}'"]
    if unit is not None and str(unit).lower() != "null" and unit != "":
        where_clauses.append(f"unit = '{unit}'")

    # Optional: time_range in spec (simple BETWEEN on startDate)
    time_range = spec.get("time_range")
    if time_range and time_range.get("start") and time_range.get("end"):
        start = time_range["start"]
        end = time_range["end"]
        where_clauses.append(f"startDate BETWEEN '{start}' AND '{end}'")

    where_sql = " AND ".join(where_clauses)

    # Value expression & aggregation
    value_expr = f"CAST({value_field} AS {cast_type.upper()})"
    agg_expr = f"{agg.upper()}({value_expr})"

    limit = spec.get("limit", 100)

    sql = f"""
SELECT
  {group_expr} AS {group_alias},
  {agg_expr} AS {metric_name}
FROM {table}
WHERE {where_sql}
GROUP BY {group_expr}
ORDER BY {group_expr}
LIMIT {limit};
""".strip()

    return sql


def run_spec(spec: HealthQuerySpec, verbose: bool = True) -> Dict[str, Any]:
    """
    Build SQL from a spec, execute it in DuckDB against
    healthkit_records.parquet, and return a JSON-friendly result.

    Returns a dict:
      {
        "sql": "<generated sql>",
        "rows": [ {<col>: <value>, ...}, ... ]
      }
    """
    metrics_cfg = load_yaml(METRICS_PATH)
    dimensions_cfg = load_yaml(DIMENSIONS_PATH)

    sql = build_health_query_sql(spec, metrics_cfg, dimensions_cfg)

    if verbose:
        print("\nGenerated SQL:")
        print(sql)

    # Connect to DuckDB in memory
    con = duckdb.connect(database=":memory:")

    # Expose the Parquet file as a view named healthkit_records
    con.execute(f"""
        CREATE VIEW healthkit_records AS
        SELECT * FROM read_parquet('{PARQUET_PATH.as_posix()}');
    """)

    # Execute the semantic query
    df = con.execute(sql).df()
    con.close()

    if verbose:
        print("\nQuery result (first 20 rows):")
        print(df.head(20))

    # JSON-friendly payload for MCP / callers
    rows = _json_safe_rows(df.to_dict(orient="records"))
    return {
        "sql": sql,
        "rows": rows,
    }

def _json_safe_rows(rows):
    """
    Convert any pandas Timestamp / datetime objects in result rows
    into plain ISO8601 strings so they can be JSON-serialized.
    """
    def convert(value):
        if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        if isinstance(value, list):
            return [convert(v) for v in value]
        return value

    return [convert(row) for row in rows]


def handle_healthkit_query(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Thin wrapper that the MCP server can call.

    `args` should match the HealthQuerySpec shape, e.g.:

      {
        "metric": "active_energy_burned",
        "aggregation": "sum",
        "time_grain": "month",
        "limit": 12,
        "time_range": {
          "start": "2021-01-01",
          "end": "2021-12-31"
        }
      }

    Returns:
      {
        "sql": "<generated SQL>",
        "rows": [ {<col>: <value>, ...}, ... ]
      }
    """
    spec: HealthQuerySpec = args  # type hint for IDE
    return run_spec(spec, verbose=False)

def main() -> None:
    metrics_cfg = load_yaml(METRICS_PATH)
    dimensions_cfg = load_yaml(DIMENSIONS_PATH)

    metrics = metrics_cfg.get("metrics", {})
    dimensions = dimensions_cfg.get("dimensions", {})

    print("Loaded metrics.yaml and dimensions.yaml")
    print(f"Number of metrics: {len(metrics)}")
    print("Metric names:")
    for name in metrics.keys():
        print(f"  - {name}")

    print("\nDimension groups:")
    for name in dimensions.keys():
        print(f"  - {name}")

    # Example: monthly active energy
    sample_spec: HealthQuerySpec = {
        "metric": "active_energy_burned",
        "aggregation": "sum",
        "time_grain": "month",
        "limit": 12,
    }

    run_spec(sample_spec)



if __name__ == "__main__":
    main()