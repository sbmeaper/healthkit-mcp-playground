import os
import json
from typing import Any, Dict, List

import streamlit as st
from openai import OpenAI

from health_query import handle_healthkit_query

# ----- OpenAI client -----
# Expect an environment variable OPENAI_API_KEY to be set.
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ----- Tool schema for the model -----
healthkit_tool = {
    "type": "function",
    "function": {
        "name": "healthkit_query",
        "description": (
            "Query the user's Apple HealthKit data via a semantic layer. "
            "Use this whenever the user asks about metrics like heart rate, "
            "steps, active energy, sleep, VO2 max, etc."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "description": (
                        "Logical metric name from metrics.yaml, e.g. "
                        "'heart_rate', 'resting_heart_rate', 'step_count', "
                        "'active_energy_burned', 'sleep_analysis_events'."
                    ),
                },
                "aggregation": {
                    "type": "string",
                    "enum": ["avg", "sum", "min", "max", "count"],
                    "description": "Aggregation to apply to the metric.",
                    "default": "avg",
                },
                "time_grain": {
                    "type": "string",
                    "enum": ["day", "week", "month"],
                    "description": "Time bucket for grouping results.",
                    "default": "day",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to return.",
                    "default": 30,
                },
                "start": {
                    "type": ["string", "null"],
                    "description": "Optional start date (YYYY-MM-DD), inclusive.",
                    "default": None,
                },
                "end": {
                    "type": ["string", "null"],
                    "description": "Optional end date (YYYY-MM-DD), inclusive.",
                    "default": None,
                },
            },
            "required": ["metric"],
        },
    },
}


def call_model_with_tools(messages: List[Dict[str, Any]]) -> str:
    """
    Send messages to the model with the healthkit_query tool available.
    If the model calls the tool, execute handle_healthkit_query() and then
    call the model again to get a natural-language answer.
    """
    # First call: let the model decide whether to call the tool
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=[healthkit_tool],
        tool_choice="auto",
    )

    msg = resp.choices[0].message

    # If no tool calls, just return the model's text
    if not msg.tool_calls:
        return msg.content or ""

    # Handle tool calls (we only expect healthkit_query here)
    tool_messages: List[Dict[str, Any]] = []
    for tool_call in msg.tool_calls:
        if tool_call.function.name != "healthkit_query":
            continue

        tool_args = json.loads(tool_call.function.arguments or "{}")

        # Map start/end to time_range for handle_healthkit_query
        spec: Dict[str, Any] = {
            "metric": tool_args.get("metric"),
            "aggregation": tool_args.get("aggregation", "avg"),
            "time_grain": tool_args.get("time_grain", "day"),
            "limit": tool_args.get("limit", 30),
        }

        start = tool_args.get("start")
        end = tool_args.get("end")
        if start and end:
            spec["time_range"] = {"start": start, "end": end}

        result = handle_healthkit_query(spec)

        # Send tool result back to the model
        tool_messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": "healthkit_query",
                "content": json.dumps(result),
            }
        )

    # Second call: give the model the tool results to explain to the user
    followup_messages = messages + [msg.to_dict()] + tool_messages

    resp2 = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=followup_messages,
    )

    return resp2.choices[0].message.content or ""


# ----- Streamlit UI -----

st.set_page_config(page_title="HealthKit Chatbot", page_icon="💓")

st.title("💓 HealthKit Chatbot")
st.write(
    "Ask questions about your Apple Health data. I can query metrics like "
    "heart rate, steps, active energy, sleep, VO₂ max, and more."
)

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant that answers questions about the "
                "user's Apple HealthKit data. When needed, use the "
                "healthkit_query tool to fetch aggregated metrics, then explain "
                "the results clearly in natural language."
            ),
        }
    ]

# Display chat history (excluding the tool/system boilerplate)
for m in st.session_state.messages:
    if m["role"] in ("user", "assistant"):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

user_input = st.chat_input("Ask a question about your HealthKit data...")

if user_input:
    # Add user message
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Call model + tools
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                answer = call_model_with_tools(st.session_state.messages)
            except Exception as e:
                answer = f"Sorry, something went wrong: {e}"

            st.markdown(answer)

    st.session_state.messages.append({"role": "assistant", "content": answer})