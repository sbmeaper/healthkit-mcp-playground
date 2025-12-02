graph TD
    %% Entities / Components
    UserChatUI[User / Chat UI<br/>(chat_app.py)] 
    LLM["LLM (OpenAI)"] 
    QuestionParser["Health-query parser<br/>(health_query.py)"] 
    MCP_Server["MCP Server<br/>(mcp_server/)"] 
    DataStore["HealthKit Data<br/>Parquet / DuckDB"] 
    Converter["HK to Parquet converter<br/>(hk_to_parquet.py)"] 

    %% Data flow / interactions
    UserChatUI -->|“Ask natural-language”| LLM
    LLM -->|“Recognizes health-data query”| QuestionParser
    QuestionParser -->|“Translate query → structured request”| MCP_Server
    MCP_Server --> DataStore
    MCP_Server -->|“Send back results”| LLM
    LLM --> UserChatUI

    %% Data ingestion path
    HEALTHXML["HealthKit export.xml"] --> Converter
    Converter --> DataStore