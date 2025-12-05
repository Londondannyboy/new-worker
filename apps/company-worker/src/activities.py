"""
Company Worker Activities

All activities for CreateCompanyWorkflow.
AI calls go through Pydantic AI Gateway.
"""

import re
import json
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional
from datetime import datetime

from temporalio import activity
from pydantic import BaseModel

# Import from packages
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from packages.ai.src.gateway import AIGateway, get_completion_async
from packages.integrations.src.research import crawl4ai_crawl, serper_search
from packages.integrations.src.storage import save_to_neon, sync_to_zep, get_from_neon


# ============================================
# PHASE 1: NORMALIZE
# ============================================

@activity.defn
async def normalize_company_url(url: str, category: str) -> Dict[str, Any]:
    """
    Normalize a company URL and extract domain.

    Returns:
        {url, domain, is_valid}
    """
    activity.logger.info(f"Normalizing URL: {url}")

    # Clean URL
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    # Extract domain
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    # Remove www prefix
    if domain.startswith("www."):
        domain = domain[4:]

    return {
        "url": url,
        "domain": domain,
        "is_valid": bool(domain),
    }


@activity.defn
async def check_company_exists(domain: str) -> bool:
    """Check if a company with this domain already exists."""
    result = await get_from_neon("companies", {"domain": domain})
    return result is not None


# ============================================
# PHASE 2: RESEARCH
# ============================================

@activity.defn
async def crawl4ai_deep_crawl(url: str, max_pages: int = 10) -> Dict[str, Any]:
    """
    Deep crawl a company website.

    Uses Crawl4AI (FREE!) to extract content from multiple pages.
    """
    activity.logger.info(f"Deep crawling: {url} (max {max_pages} pages)")

    # Crawl main page
    main_result = await crawl4ai_crawl(url, depth=1, max_pages=1)

    if not main_result.get("success"):
        return {"pages": [], "error": main_result.get("error")}

    pages = [{
        "url": url,
        "title": main_result.get("title", ""),
        "content": main_result.get("content", ""),
    }]

    # Crawl internal links
    internal_links = main_result.get("links", [])[:max_pages - 1]

    for link in internal_links:
        try:
            link_result = await crawl4ai_crawl(link, depth=1, max_pages=1)
            if link_result.get("success") and link_result.get("content"):
                pages.append({
                    "url": link,
                    "title": link_result.get("title", ""),
                    "content": link_result.get("content", "")[:5000],  # Limit size
                })
        except Exception as e:
            activity.logger.warning(f"Failed to crawl {link}: {e}")

    activity.logger.info(f"Crawled {len(pages)} pages")

    return {
        "pages": pages,
        "cost": 0.0,  # Crawl4AI is FREE!
    }


@activity.defn
async def serper_company_search(domain: str) -> Dict[str, Any]:
    """
    Search for company news and mentions.

    Uses Serper API (~$0.001 per search).
    """
    activity.logger.info(f"Searching news for: {domain}")

    # News search
    news_result = await serper_search(
        query=f"{domain} news",
        search_type="news",
        num_results=10,
    )

    return {
        "results": news_result.get("results", []),
        "cost": news_result.get("cost", 0.001),
    }


# ============================================
# PHASE 3: CURATE
# ============================================

class CurationResult(BaseModel):
    """Result of source curation."""
    sources: List[Dict[str, Any]]
    confidence: float
    key_facts: List[str]


@activity.defn
async def curate_research_sources(
    crawled_pages: List[Dict[str, Any]],
    news_articles: List[Dict[str, Any]],
    category: str,
) -> Dict[str, Any]:
    """
    AI curates and ranks research sources.

    Uses Pydantic AI Gateway (Groq for speed/cost).
    """
    activity.logger.info("Curating research sources via AI Gateway")

    # Build context from sources
    context_parts = []

    for page in crawled_pages[:5]:
        context_parts.append(f"[Website] {page.get('title', 'Page')}:\n{page.get('content', '')[:2000]}")

    for article in news_articles[:5]:
        context_parts.append(f"[News] {article.get('title', '')}:\n{article.get('snippet', '')}")

    context = "\n\n---\n\n".join(context_parts)

    # AI curation prompt
    prompt = f"""Analyze these research sources about a company (category: {category}).

{context}

Rate the quality and relevance of each source.
Extract the key facts about the company.
Provide a confidence score (0-1) for how much we know.

Respond with JSON:
{{
  "sources": [
    {{"url": "...", "quality": 0.9, "relevance": 0.8, "summary": "..."}}
  ],
  "confidence": 0.7,
  "key_facts": ["fact 1", "fact 2"]
}}"""

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(
            prompt=prompt,
            response_model=CurationResult,
            model="fast",  # Groq llama-3.1-8b (cheap + fast)
        )

        return {
            "sources": [s for s in result.sources],
            "confidence": result.confidence,
            "key_facts": result.key_facts,
            "cost": 0.001,  # Groq is very cheap
        }

    except Exception as e:
        activity.logger.error(f"Curation failed: {e}")
        # Fallback: return all sources unranked
        return {
            "sources": [
                {"url": p.get("url"), "content": p.get("content", "")[:2000]}
                for p in crawled_pages
            ],
            "confidence": 0.5,
            "key_facts": [],
            "cost": 0,
        }
    finally:
        await gateway.close()


# ============================================
# PHASE 4: CHECK AMBIGUITY
# ============================================

@activity.defn
async def check_research_ambiguity(
    sources: List[Dict[str, Any]],
    domain: str,
) -> Dict[str, Any]:
    """
    Check if research is ambiguous (multiple companies? unclear identity?).

    Returns signals if human review is needed.
    """
    activity.logger.info("Checking research ambiguity")

    # Simple heuristics
    signals = []

    if len(sources) < 2:
        signals.append("insufficient_sources")

    # Check for conflicting information
    # (In production: use AI to detect conflicts)

    return {
        "is_ambiguous": len(signals) > 0 and "insufficient_sources" in signals,
        "signals": signals,
    }


# ============================================
# PHASE 5: GENERATE PROFILE
# ============================================

class CompanyProfileOutput(BaseModel):
    """AI-generated company profile."""
    legal_name: str
    slug: str
    tagline: Optional[str] = None
    about: str
    what_we_do: Optional[str] = None
    category: str
    sub_category: Optional[str] = None
    founded_year: Optional[int] = None
    headquarters: Optional[str] = None
    services: List[str] = []
    sectors: List[str] = []
    key_clients: List[str] = []


@activity.defn
async def generate_company_profile(
    sources: List[Dict[str, Any]],
    domain: str,
    category: str,
    app: str,
    jurisdiction: str,
) -> Dict[str, Any]:
    """
    Generate company profile using Pydantic AI Gateway.

    Uses GPT-4o-mini for quality generation.
    """
    activity.logger.info(f"Generating profile for {domain} via AI Gateway")

    # Build context
    context_parts = []
    for source in sources[:8]:
        content = source.get("content", source.get("summary", ""))[:2000]
        context_parts.append(content)

    context = "\n\n".join(context_parts)

    # Profile generation prompt
    prompt = f"""Create a comprehensive company profile for {domain}.

Research Context:
{context}

Category hint: {category}
Jurisdiction: {jurisdiction}

Generate a professional profile with:
- legal_name: Official company name
- slug: URL-safe lowercase identifier (e.g., "acme-corp")
- tagline: Short company tagline if available
- about: 2-3 paragraph narrative about the company
- what_we_do: What services/products they offer
- category: Primary business category
- services: List of key services
- sectors: Industry sectors they operate in
- key_clients: Notable clients if mentioned

Be factual and only include information supported by the research."""

    gateway = AIGateway()
    try:
        profile = await gateway.structured_output(
            prompt=prompt,
            response_model=CompanyProfileOutput,
            model="quick",  # GPT-4o-mini for quality
            system_prompt="You are a professional business analyst creating company profiles.",
        )

        return {
            "success": True,
            "profile": profile.model_dump(),
            "cost": 0.005,  # GPT-4o-mini cost
        }

    except Exception as e:
        activity.logger.error(f"Profile generation failed: {e}")
        return {
            "success": False,
            "error": str(e),
        }
    finally:
        await gateway.close()


# ============================================
# PHASE 6: MEDIA
# ============================================

@activity.defn
async def extract_and_process_logo(url: str, company_name: str) -> Dict[str, Any]:
    """
    Extract company logo from website.

    TODO: Implement logo extraction and upload to Cloudinary.
    """
    activity.logger.info(f"Extracting logo for {company_name}")

    # Placeholder - in production, would:
    # 1. Crawl website for logo
    # 2. Use Clearbit/similar API
    # 3. Upload to Cloudinary

    return {
        "logo_url": None,
        "source": "not_implemented",
    }


# ============================================
# PHASE 7: SAVE
# ============================================

@activity.defn
async def save_company_to_neon(profile: Dict[str, Any], app: str) -> Dict[str, Any]:
    """
    Save company profile to Neon PostgreSQL.
    """
    activity.logger.info(f"Saving company: {profile.get('slug')}")

    # Prepare data for companies table
    data = {
        "slug": profile.get("slug"),
        "domain": profile.get("domain", profile.get("slug", "").replace("-", ".")),
        "name": profile.get("legal_name"),
        "type": profile.get("category", "company"),
        "app": app,
        "payload": json.dumps(profile),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    result = await save_to_neon("companies", data, on_conflict="slug")

    return result


@activity.defn
async def sync_company_to_zep(
    company_id: str,
    profile: Dict[str, Any],
    app: str,
) -> Dict[str, Any]:
    """
    Sync company to Zep knowledge graph.
    """
    activity.logger.info(f"Syncing company to Zep: {company_id}")

    # Build content for Zep
    content = f"""Company: {profile.get('legal_name')}
Category: {profile.get('category')}
About: {profile.get('about', '')}
Services: {', '.join(profile.get('services', []))}
Sectors: {', '.join(profile.get('sectors', []))}"""

    # Map app to graph
    graph_mapping = {
        "placement": "finance-knowledge",
        "relocation": "relocation",
        "jobs": "jobs",
    }
    graph_id = graph_mapping.get(app, "finance-knowledge")

    result = await sync_to_zep(
        entity_id=company_id,
        entity_type="company",
        content=content,
        graph_id=graph_id,
        metadata={"slug": profile.get("slug")},
    )

    return result
