"""
Pydantic AI Gateway Client

Unified access to AI providers via Pydantic Gateway.
Single API key (paig_xxx) for all providers.

Supported providers via gateway:
- OpenAI (gpt-4o, gpt-4o-mini)
- Groq (llama-3.1-8b-instant, llama-3.1-70b-versatile) - CHEAPER!
- Anthropic (claude-3-5-haiku, claude-sonnet-4-5)

URL Patterns (CRITICAL):
- OpenAI: https://gateway.pydantic.dev/proxy/openai/chat/completions
- Groq: https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions
- Anthropic: https://gateway.pydantic.dev/proxy/anthropic/v1/messages
"""

import os
import json
import httpx
from typing import Any, Optional, TypeVar, Type
from pydantic import BaseModel
import structlog

logger = structlog.get_logger()

T = TypeVar("T", bound=BaseModel)


# Gateway endpoints
GATEWAY_ENDPOINTS = {
    # OpenAI models - note: NO /v1/ in path
    "gpt-4o": "https://gateway.pydantic.dev/proxy/openai/chat/completions",
    "gpt-4o-mini": "https://gateway.pydantic.dev/proxy/openai/chat/completions",
    "gpt-4-turbo": "https://gateway.pydantic.dev/proxy/openai/chat/completions",

    # Groq models (CHEAPER!) - note: HAS /openai/v1/ in path
    "llama-3.1-8b-instant": "https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions",
    "llama-3.1-70b-versatile": "https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions",
    "llama-3.3-70b-versatile": "https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions",

    # Anthropic models - different response format
    "claude-3-5-haiku-latest": "https://gateway.pydantic.dev/proxy/anthropic/v1/messages",
    "claude-sonnet-4-5": "https://gateway.pydantic.dev/proxy/anthropic/v1/messages",
}

# Model aliases for convenience
MODEL_ALIASES = {
    "fast": "llama-3.1-8b-instant",  # Cheapest, fastest
    "quick": "gpt-4o-mini",
    "quality": "gpt-4o",
    "smart": "claude-sonnet-4-5",
    "haiku": "claude-3-5-haiku-latest",
}


class AIGateway:
    """
    Pydantic AI Gateway client.

    Usage:
        gateway = AIGateway()
        response = await gateway.complete("What is 2+2?")
        structured = await gateway.structured_output(query, ResponseModel)
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("PYDANTIC_AI_GATEWAY_API_KEY")
        if not self.api_key:
            raise ValueError("PYDANTIC_AI_GATEWAY_API_KEY not set")

        self._client = httpx.AsyncClient(timeout=120.0)

    def _resolve_model(self, model: str) -> str:
        """Resolve model alias to actual model name."""
        return MODEL_ALIASES.get(model, model)

    def _get_endpoint(self, model: str) -> str:
        """Get the correct endpoint URL for a model."""
        resolved = self._resolve_model(model)
        if resolved not in GATEWAY_ENDPOINTS:
            # Default to OpenAI endpoint for unknown models
            return GATEWAY_ENDPOINTS["gpt-4o-mini"]
        return GATEWAY_ENDPOINTS[resolved]

    def _is_anthropic(self, model: str) -> bool:
        """Check if model uses Anthropic API format."""
        resolved = self._resolve_model(model)
        return resolved.startswith("claude")

    async def complete(
        self,
        prompt: str,
        model: str = "fast",
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> str:
        """
        Get a text completion from AI.

        Args:
            prompt: User prompt
            model: Model name or alias (fast, quick, quality, smart)
            system_prompt: Optional system prompt
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text response
        """
        resolved_model = self._resolve_model(model)
        endpoint = self._get_endpoint(model)

        logger.info(
            "ai_gateway_request",
            model=resolved_model,
            endpoint=endpoint,
            prompt_length=len(prompt),
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        if self._is_anthropic(model):
            # Anthropic format
            headers["anthropic-version"] = "2023-06-01"
            body = {
                "model": resolved_model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            }
            if system_prompt:
                body["system"] = system_prompt
        else:
            # OpenAI format (works for OpenAI and Groq)
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            body = {
                "model": resolved_model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }

        response = await self._client.post(endpoint, headers=headers, json=body)

        if not response.is_success:
            logger.error(
                "ai_gateway_error",
                status=response.status_code,
                error=response.text,
            )
            raise RuntimeError(f"AI Gateway error: {response.status_code} - {response.text}")

        data = response.json()

        # Extract content based on response format
        if self._is_anthropic(model):
            content = data.get("content", [{}])[0].get("text", "")
        else:
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

        logger.info(
            "ai_gateway_response",
            model=resolved_model,
            response_length=len(content),
        )

        return content

    async def structured_output(
        self,
        prompt: str,
        response_model: Type[T],
        model: str = "fast",
        system_prompt: Optional[str] = None,
    ) -> T:
        """
        Get structured output that validates against a Pydantic model.

        Args:
            prompt: User prompt
            response_model: Pydantic model class for response validation
            model: Model name or alias
            system_prompt: Optional system prompt

        Returns:
            Validated Pydantic model instance
        """
        # Build schema-aware system prompt
        schema = response_model.model_json_schema()
        schema_str = json.dumps(schema, indent=2)

        full_system = f"""You must respond with valid JSON that matches this schema:

{schema_str}

Respond ONLY with the JSON object, no explanation or markdown."""

        if system_prompt:
            full_system = f"{system_prompt}\n\n{full_system}"

        # Get completion
        response_text = await self.complete(
            prompt=prompt,
            model=model,
            system_prompt=full_system,
            temperature=0.1,  # Lower for structured output
        )

        # Clean response (remove markdown code blocks if present)
        cleaned = response_text.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        # Parse and validate
        try:
            data = json.loads(cleaned)
            return response_model.model_validate(data)
        except (json.JSONDecodeError, Exception) as e:
            logger.error(
                "structured_output_parse_error",
                error=str(e),
                response=response_text[:500],
            )
            raise ValueError(f"Failed to parse structured output: {e}")

    async def close(self):
        """Close the HTTP client."""
        await self._client.aclose()


# Module-level convenience functions
_gateway: Optional[AIGateway] = None


def _get_gateway() -> AIGateway:
    global _gateway
    if _gateway is None:
        _gateway = AIGateway()
    return _gateway


async def get_completion_async(
    prompt: str,
    model: str = "fast",
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4096,
) -> str:
    """Get a completion from AI (async)."""
    return await _get_gateway().complete(
        prompt=prompt,
        model=model,
        system_prompt=system_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def get_completion(
    prompt: str,
    model: str = "fast",
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4096,
) -> str:
    """Get a completion from AI (sync wrapper)."""
    import asyncio
    return asyncio.run(get_completion_async(
        prompt=prompt,
        model=model,
        system_prompt=system_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
    ))


async def get_structured_output_async(
    prompt: str,
    response_model: Type[T],
    model: str = "fast",
    system_prompt: Optional[str] = None,
) -> T:
    """Get structured output from AI (async)."""
    return await _get_gateway().structured_output(
        prompt=prompt,
        response_model=response_model,
        model=model,
        system_prompt=system_prompt,
    )


def get_structured_output(
    prompt: str,
    response_model: Type[T],
    model: str = "fast",
    system_prompt: Optional[str] = None,
) -> T:
    """Get structured output from AI (sync wrapper)."""
    import asyncio
    return asyncio.run(get_structured_output_async(
        prompt=prompt,
        response_model=response_model,
        model=model,
        system_prompt=system_prompt,
    ))
