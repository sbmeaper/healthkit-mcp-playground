graph TD
    A[User] --> B[Chat UI]
    B --> C[LLM]
    C --> D[MCP Server - HealthKit]
    D --> E[Health Data Store]
    E --> D
    D --> C
    C --> B

    F[HealthKit export.xml] --> G[HK to Parquet Converter]
    G --> E