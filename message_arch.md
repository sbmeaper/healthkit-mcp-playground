```mermaid
sequenceDiagram
    participant U as User
    participant UI as Chat UI
    participant LLM as LLM
    participant HQ as health_query
    participant MCP as MCP server
    participant DB as Health Data Store

    U->>UI: Type natural-language question
    UI->>LLM: Send chat request\n(prompt + tools/metrics schema)
    LLM-->>UI: Tool call\n(e.g. get_metric with filters)
    UI->>HQ: Pass tool call\n(parse & validate)
    HQ->>MCP: Request metric\n(metric_id, dimensions, filters)
    MCP->>DB: Execute query\n(SQL over DuckDB/Parquet)
    DB-->>MCP: Result rows / aggregates
    MCP-->>HQ: Metric result
    HQ-->>UI: Tool result JSON
    UI->>LLM: Send tool result\n(as tool/function response)
    LLM-->>UI: Final natural-language answer
    UI-->>U: Display answer in chat