#!/usr/bin/env python3
"""
Test Article Workflow Activities

Tests key article generation activities through the Pydantic AI Gateway.
"""

import os
import asyncio
import sys
from pathlib import Path

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Load env from parent quest directory
load_dotenv(Path(__file__).parent.parent / ".env")

from packages.ai.src.gateway import AIGateway


async def test_keyword_research():
    """Test keyword research via AI."""
    print("\n" + "="*50)
    print("TEST 1: Keyword Research")
    print("="*50)

    from pydantic import BaseModel
    from typing import List, Optional

    class KeywordResult(BaseModel):
        target_keyword: str
        volume: Optional[int] = None
        difficulty: Optional[float] = None
        secondary_keywords: List[str] = []

    gateway = AIGateway()
    try:
        topic = "Private Equity Trends 2024"
        prompt = f"""Suggest SEO keywords for an article about: {topic}

Target jurisdiction: UK

Return:
- target_keyword: The primary keyword to target
- secondary_keywords: 3-5 related keywords
- volume: Estimated monthly search volume (guess based on topic)
- difficulty: Keyword difficulty 0-1 (guess based on competition)"""

        result = await gateway.structured_output(
            prompt=prompt,
            response_model=KeywordResult,
            model="fast",
        )

        print(f"Target keyword: {result.target_keyword}")
        print(f"Volume: {result.volume}")
        print(f"Difficulty: {result.difficulty}")
        print(f"Secondary: {result.secondary_keywords}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        await gateway.close()


async def test_source_curation():
    """Test source curation via AI."""
    print("\n" + "="*50)
    print("TEST 2: Source Curation")
    print("="*50)

    from pydantic import BaseModel
    from typing import List, Dict, Any

    class SourceCuration(BaseModel):
        sources: List[Dict[str, Any]]
        confidence: float
        key_facts: List[str]
        perspectives: List[str]
        outline: List[str]

    gateway = AIGateway()
    try:
        # Simulated research content
        context = """[Page] Private Equity Overview:
Private equity firms manage over $5 trillion in assets globally.
2024 sees increased focus on AI and technology investments.
Deal flow has slowed due to higher interest rates.

[News] PE Firms Look to AI:
Major PE firms are increasing allocations to AI startups.
Valuations have corrected from 2021 highs.
Mid-market deals remain attractive."""

        prompt = f"""Analyze these research sources for an article about: Private Equity Trends 2024

{context}

Curate the sources:
1. Rate quality (0-1) and relevance (0-1) of each
2. Extract key facts
3. Identify different perspectives
4. Suggest an article outline

Return JSON with sources, confidence, key_facts, perspectives, outline."""

        result = await gateway.structured_output(
            prompt=prompt,
            response_model=SourceCuration,
            model="fast",
        )

        print(f"Sources: {len(result.sources)}")
        print(f"Confidence: {result.confidence}")
        print(f"Key facts: {result.key_facts[:3]}")
        print(f"Outline: {result.outline}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        await gateway.close()


async def test_article_generation():
    """Test 4-act article generation via AI."""
    print("\n" + "="*50)
    print("TEST 3: 4-Act Article Generation (GPT-4o-mini)")
    print("="*50)

    from pydantic import BaseModel
    from typing import List, Optional

    class ArticleSection(BaseModel):
        title: str
        content: str
        factoid: Optional[str] = None
        visual_hint: str

    class GeneratedArticle(BaseModel):
        title: str
        slug: str
        excerpt: str
        sections: List[ArticleSection]

    gateway = AIGateway()
    try:
        prompt = """Write a professional 500-word article about: Private Equity Trends 2024

RESEARCH CONTEXT:
Private equity firms manage over $5 trillion globally. 2024 sees increased focus on AI investments. Deal flow has slowed due to higher interest rates. Mid-market deals remain attractive.

KEY FACTS:
- PE assets under management exceed $5 trillion
- AI investments are a major focus
- Higher rates affecting deal flow
- Mid-market showing resilience

SUGGESTED OUTLINE:
1. Market overview
2. AI investment trends
3. Impact of interest rates
4. Outlook for 2025

TARGET KEYWORD: private equity trends 2024
ARTICLE TYPE: standard
APP: placement

Write exactly 4 sections (4-Act structure).
Each section ~125 words.
Include a factoid and 45-55 word visual_hint for each section.

Return: title, slug, excerpt, sections (array of 4)."""

        result = await gateway.structured_output(
            prompt=prompt,
            response_model=GeneratedArticle,
            model="quick",  # GPT-4o-mini
            system_prompt="You are a professional financial journalist.",
        )

        print(f"Title: {result.title}")
        print(f"Slug: {result.slug}")
        print(f"Excerpt: {result.excerpt[:100]}...")
        print(f"Sections: {len(result.sections)}")

        for i, section in enumerate(result.sections):
            print(f"\n  Section {i+1}: {section.title}")
            print(f"  Content: {section.content[:80]}...")
            print(f"  Visual hint: {section.visual_hint[:60]}...")

        return len(result.sections) == 4
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await gateway.close()


async def main():
    """Run all tests."""
    print("\n Article Workflow Test Suite")
    print("="*50)

    # Check API key
    api_key = os.getenv("PYDANTIC_AI_GATEWAY_API_KEY")
    if not api_key:
        print("PYDANTIC_AI_GATEWAY_API_KEY not set!")
        return

    print(f"API Key: {api_key[:10]}...")

    # Run tests
    results = {
        "Keyword Research": await test_keyword_research(),
        "Source Curation": await test_source_curation(),
        "Article Generation": await test_article_generation(),
    }

    # Summary
    print("\n" + "="*50)
    print("RESULTS SUMMARY")
    print("="*50)

    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"   {status}: {test_name}")

    all_passed = all(results.values())
    print("\n" + ("All tests passed!" if all_passed else "Some tests failed"))

    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
