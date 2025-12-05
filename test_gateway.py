#!/usr/bin/env python3
"""
Test Pydantic AI Gateway

Verifies the gateway client works with multiple providers.
"""

import os
import asyncio
import sys
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Load env from parent quest directory
load_dotenv(Path(__file__).parent.parent / ".env")

from packages.ai.src.gateway import AIGateway, get_completion_async


class TestResult(BaseModel):
    """Structured output test model."""
    answer: int
    explanation: str


class CompanyInfo(BaseModel):
    """Test company extraction model."""
    name: str
    industry: str
    key_facts: List[str]


async def test_basic_completion():
    """Test basic text completion."""
    print("\n" + "="*50)
    print("TEST 1: Basic Completion (Groq - fast)")
    print("="*50)

    try:
        response = await get_completion_async(
            prompt="What is 2+2? Answer with just the number.",
            model="fast",  # Uses Groq llama-3.1-8b
        )
        print(f"✅ Response: {response.strip()}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_structured_output():
    """Test structured output with Pydantic model."""
    print("\n" + "="*50)
    print("TEST 2: Structured Output (Groq)")
    print("="*50)

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(
            prompt="What is 15 + 27? Provide the answer and a brief explanation.",
            response_model=TestResult,
            model="fast",
        )
        print(f"✅ Answer: {result.answer}")
        print(f"✅ Explanation: {result.explanation}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        await gateway.close()


async def test_company_extraction():
    """Test company info extraction (simulates profile generation)."""
    print("\n" + "="*50)
    print("TEST 3: Company Extraction (GPT-4o-mini)")
    print("="*50)

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(
            prompt="""Extract company information from this text:

Anthropic is an AI safety company founded in 2021. They created Claude,
a helpful AI assistant. The company focuses on AI alignment research
and building safe AI systems. Key facts: Series C funding of $450M,
headquarters in San Francisco, founded by former OpenAI researchers.""",
            response_model=CompanyInfo,
            model="quick",  # GPT-4o-mini
        )
        print(f"✅ Name: {result.name}")
        print(f"✅ Industry: {result.industry}")
        print(f"✅ Key Facts: {result.key_facts}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        await gateway.close()


async def test_different_providers():
    """Test different providers through the gateway."""
    print("\n" + "="*50)
    print("TEST 4: Multiple Providers")
    print("="*50)

    gateway = AIGateway()
    providers = [
        ("fast", "Groq llama-3.1-8b"),
        ("quick", "OpenAI GPT-4o-mini"),
    ]

    results = []
    for model, name in providers:
        try:
            response = await gateway.complete(
                prompt="Say 'Hello' and nothing else.",
                model=model,
                max_tokens=10,
            )
            print(f"✅ {name}: {response.strip()}")
            results.append(True)
        except Exception as e:
            print(f"❌ {name}: {e}")
            results.append(False)

    await gateway.close()
    return all(results)


async def main():
    """Run all tests."""
    print("\n🚀 Pydantic AI Gateway Test Suite")
    print("="*50)

    # Check API key
    api_key = os.getenv("PYDANTIC_AI_GATEWAY_API_KEY")
    if not api_key:
        print("❌ PYDANTIC_AI_GATEWAY_API_KEY not set!")
        print("   Set it in quest/.env")
        return

    print(f"🔑 API Key: {api_key[:10]}...")

    # Run tests
    results = {
        "Basic Completion": await test_basic_completion(),
        "Structured Output": await test_structured_output(),
        "Company Extraction": await test_company_extraction(),
        "Multiple Providers": await test_different_providers(),
    }

    # Summary
    print("\n" + "="*50)
    print("📊 RESULTS SUMMARY")
    print("="*50)

    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {test_name}")

    all_passed = all(results.values())
    print("\n" + ("🎉 All tests passed!" if all_passed else "⚠️  Some tests failed"))

    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
