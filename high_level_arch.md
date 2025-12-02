graph TD
  User[User] --> ChatUI[Chat UI]
  ChatUI --> LLM[LLM OpenAI]
  LLM --> MCP[MCP Server - HealthKit]
  MCP --> Data[HealthKit Data Store]
  Data --> MCP
  MCP --> LLM
  LLM --> ChatUI

  HKXML[HealthKit export.xml] --> Converter[HK to Parquet Converter]
  Converter --> Data