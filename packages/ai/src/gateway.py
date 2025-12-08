"""
Pydantic AI Gateway Client

Unified access to AI providers via Pydantic Gateway + Google Gemini.

Supported providers:
- OpenAI (gpt-4o, gpt-4o-mini) via Pydantic Gateway
- Groq (llama-3.1-8b-instant, llama-3.1-70b-versatile) via Pydantic Gateway - CHEAPER!
- Google Gemini (gemini-2.5-pro, gemini-2.5-flash) via Google's OpenAI-compatible API

URL Patterns (CRITICAL):
- OpenAI: https://gateway.pydantic.dev/proxy/openai/chat/completions
- Groq: https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions
- Google: https://generativelanguage.googleapis.com/v1beta/openai/chat/completions

API Keys:
- PYDANTIC_AI_GATEWAY_API_KEY for OpenAI/Groq via gateway
- GOOGLE_API_KEY for Gemini models
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
    # OpenAI models via Pydantic Gateway - note: NO /v1/ in path
    "gpt-4o": "https://gateway.pydantic.dev/proxy/openai/chat/completions",
    "gpt-4o-mini": "https://gateway.pydantic.dev/proxy/openai/chat/completions",
    "gpt-4-turbo": "https://gateway.pydantic.dev/proxy/openai/chat/completions",

    # Groq models via Pydantic Gateway (CHEAPER!) - note: HAS /openai/v1/ in path
    "llama-3.1-8b-instant": "https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions",
    "llama-3.1-70b-versatile": "https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions",
    "llama-3.3-70b-versatile": "https://gateway.pydantic.dev/proxy/groq/openai/v1/chat/completions",

    # Google Gemini models via Google's OpenAI-compatible API (BEST QUALITY!)
    "gemini-2.5-pro": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
    "gemini-2.5-flash": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
    "gemini-2.0-flash": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
}

# Model aliases for convenience
MODEL_ALIASES = {
    "fast": "llama-3.1-8b-instant",  # Cheapest, fastest (Groq)
    "quick": "gpt-4o-mini",           # Fast OpenAI
    "quality": "gpt-4o",              # Best OpenAI
    "smart": "gemini-2.5-pro",        # Best Gemini (replaces Claude)
    "flash": "gemini-2.5-flash",      # Fast Gemini
}


class AIGateway:
    """
    AI Gateway client supporting multiple providers.

    Usage:
        gateway = AIGateway()
        response = await gateway.complete("What is 2+2?")
        structured = await gateway.structured_output(query, ResponseModel)
    """

    def __init__(
        self,
        gateway_api_key: Optional[str] = None,
        google_api_key: Optional[str] = None,
    ):
        # Pydantic Gateway key for OpenAI/Groq
        self.gateway_api_key = gateway_api_key or os.getenv("PYDANTIC_AI_GATEWAY_API_KEY")
        # Google API key for Gemini models
        self.google_api_key = google_api_key or os.getenv("GOOGLE_API_KEY")

        # At least one key must be set
        if not self.gateway_api_key and not self.google_api_key:
            raise ValueError("No API keys set. Need PYDANTIC_AI_GATEWAY_API_KEY or GOOGLE_API_KEY")

        self._client = httpx.AsyncClient(timeout=120.0)

    def _resolve_model(self, model: str) -> str:
        """Resolve model alias to actual model name."""
        return MODEL_ALIASES.get(model, model)

    def _get_endpoint(self, model: str) -> str:
        """Get the correct endpoint URL for a model."""
        resolved = self._resolve_model(model)
        if resolved not in GATEWAY_ENDPOINTS:
            # Default to Gemini for unknown models (since Anthropic is unavailable)
            return GATEWAY_ENDPOINTS["gemini-2.5-flash"]
        return GATEWAY_ENDPOINTS[resolved]

    def _is_google(self, model: str) -> bool:
        """Check if model is a Google Gemini model."""
        resolved = self._resolve_model(model)
        return resolved.startswith("gemini")

    def _get_api_key(self, model: str) -> str:
        """Get the appropriate API key for a model."""
        if self._is_google(model):
            if not self.google_api_key:
                raise ValueError("GOOGLE_API_KEY not set for Gemini model")
            return self.google_api_key
        else:
            if not self.gateway_api_key:
                raise ValueError("PYDANTIC_AI_GATEWAY_API_KEY not set for gateway model")
            return self.gateway_api_key

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
        api_key = self._get_api_key(model)

        logger.info(
            "ai_gateway_request",
            model=resolved_model,
            endpoint=endpoint,
            prompt_length=len(prompt),
            provider="google" if self._is_google(model) else "gateway",
        )

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # All providers now use OpenAI-compatible format
        # (Pydantic Gateway for OpenAI/Groq, Google's OpenAI-compatible API for Gemini)
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
                model=resolved_model,
            )
            raise RuntimeError(f"AI Gateway error: {response.status_code} - {response.text}")

        data = response.json()

        # Extract content - OpenAI-compatible format for all providers
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
        # Get field names and descriptions for clearer instructions
        schema = response_model.model_json_schema()
        properties = schema.get("properties", {})
        required = schema.get("required", [])

        # Build field list for clarity
        field_list = []
        for name, prop in properties.items():
            req = "(required)" if name in required else "(optional)"
            desc = prop.get("description", "")
            ftype = prop.get("type", "")
            field_list.append(f"- {name} {req}: {desc}")

        fields_str = "\n".join(field_list)

        full_system = f"""You must respond with a valid JSON object containing these fields:

{fields_str}

IMPORTANT:
- Return ONLY the JSON data object, NOT a schema
- Do NOT wrap your response in "properties" or "description"
- Start your response with {{ and end with }}
- Include actual values for each field based on the prompt"""

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

            # Handle case where AI wrapped data in schema format
            if "properties" in data and "description" in data:
                # AI returned schema-like format, extract from properties
                data = data["properties"]

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
