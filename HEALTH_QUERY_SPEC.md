# HealthQuerySpec contract

This is the JSON shape used by the HealthKit MCP tool to query my HealthKit semantic layer.

```jsonc
{
  "metric": "active_energy_burned",        // required: key from metrics.yaml
  "aggregation": "sum",                    // one of: "avg", "sum", "min", "max", "count"
  "time_grain": "month",                   // one of: "day", "week", "month"
  "limit": 12,                             // optional, default = 100
  "time_range": {                          // optional
    "start": "2021-01-01",                 // YYYY-MM-DD, filters startDate >= start
    "end":   "2021-12-31"                  // YYYY-MM-DD, filters startDate <= end
  }
}