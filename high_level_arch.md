graph TD
  User[User] --> ChatUI[Chat UI]
  ChatUI --> LLM[LLM (OpenAI)]
  LLM --> MCP[MCP Server (HealthKit)]
  MCP --> Data[HealthKit Data Store<br/>(Parquet / DuckDB)]
  Data --> MCP
  MCP --> LLM
  LLM --> ChatUI

  HKXML[HealthKit export.xml] --> Converter[HK → Parquet Converter]
  Converter --> Data