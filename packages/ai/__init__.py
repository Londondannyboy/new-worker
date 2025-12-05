"""
Quest AI Package

Unified AI access via Pydantic AI Gateway.
All AI calls go through the gateway for:
- Cost tracking
- Observability (LogFire)
- Provider flexibility (OpenAI, Groq, Anthropic)
"""

from .src.gateway import (
    AIGateway,
    get_completion,
    get_completion_async,
    get_structured_output,
    get_structured_output_async,
)

__all__ = [
    "AIGateway",
    "get_completion",
    "get_completion_async",
    "get_structured_output",
    "get_structured_output_async",
]
