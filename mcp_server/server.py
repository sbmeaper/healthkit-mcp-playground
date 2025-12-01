"""
MCP server for querying my Apple HealthKit data.

Exposes a single tool:
  - healthkit_query

which forwards to health_query.handle_healthkit_query()
and runs semantic SQL over healthkit_records.parquet via DuckDB.
"""

from typing import Any, Optional

from mcp.server.fastmcp import FastMCP
from health_query import handle_healthkit_query

# Create the MCP server instance
mcp = FastMCP(name="HealthKit", json_response=True)


@mcp.tool()
def healthkit_query(
    metric: str,
    aggregation: str = "avg",
    time_grain: str = "day",
    limit: int = 100,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict[str, Any]:
    """
    Query my Apple HealthKit data using the semantic layer.

    Parameters
    ----------
    metric:
        Logical metric name from metrics.yaml
        (e.g. "heart_rate", "step_count", "active_energy_burned").
    aggregation:
        Aggregation function: "avg", "sum", "min", "max", or "count".
    time_grain:
        Time bucket: "day", "week", or "month".
    limit:
        Maximum number of rows to return.
    start:
        Optional start date (YYYY-MM-DD), inclusive.
    end:
        Optional end date (YYYY-MM-DD), inclusive.

    Returns
    -------
    dict with:
      - "sql": the generated SQL query string
      - "rows": list of result records, each a {column: value} dict
    """
    spec: dict[str, Any] = {
        "metric": metric,
        "aggregation": aggregation,
        "time_grain": time_grain,
        "limit": limit,
    }

    if start and end:
        spec["time_range"] = {"start": start, "end": end}

    # Delegate to your existing semantic query engine
    return handle_healthkit_query(spec)


if __name__ == "__main__":
    # Run the MCP server over stdio (default); external tools
    # like MCP Inspector or ChatGPT will launch this process.
    mcp.run()