"""
Content Worker Activities

All activities for CreateCompanyWorkflow and CreateArticleWorkflow.
AI calls go through Pydantic AI Gateway.
"""

import re
import json
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

from temporalio import activity
from pydantic import BaseModel

# Add packages to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from packages.ai.src.gateway import AIGateway, get_completion_async
from packages.integrations.src.research import crawl4ai_crawl, serper_search
from packages.integrations.src.storage import save_to_neon, sync_to_zep, get_from_neon


# ============================================
# COMPANY ACTIVITIES
# ============================================

@activity.defn
async def normalize_company_url(url: str, category: str) -> Dict[str, Any]:
    """Normalize a company URL and extract domain."""
    activity.logger.info(f"Normalizing URL: {url}")
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return {"url": url, "domain": domain, "is_valid": bool(domain)}


@activity.defn
async def check_company_exists(domain: str) -> Dict[str, Any]:
    """Check if a company with this domain already exists in payload->>'website'."""
    activity.logger.info(f"Checking if company exists: {domain}")
    import os
    import asyncpg

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"exists": False}

    try:
        conn = await asyncpg.connect(database_url)
        try:
            row = await conn.fetchrow("""
                SELECT id, slug, updated_at
                FROM companies
                WHERE payload->>'website' LIKE $1
                OR payload->>'website' LIKE $2
                LIMIT 1
            """, f"%{domain}%", f"%www.{domain}%")

            if row:
                activity.logger.info(f"Company exists: {row['slug']} (ID: {row['id']})")
                return {
                    "exists": True,
                    "company_id": str(row["id"]),
                    "slug": row["slug"],
                }
            else:
                return {"exists": False}
        finally:
            await conn.close()
    except Exception as e:
        activity.logger.error(f"Error checking company exists: {e}")
        return {"exists": False}


@activity.defn
async def crawl4ai_deep_crawl(url: str, max_pages: int = 10) -> Dict[str, Any]:
    """Deep crawl a company website using Crawl4AI."""
    activity.logger.info(f"Deep crawling: {url}")
    main_result = await crawl4ai_crawl(url, depth=1, max_pages=1)
    if not main_result.get("success"):
        return {"pages": [], "error": main_result.get("error")}
    pages = [{"url": url, "title": main_result.get("title", ""), "content": main_result.get("content", "")}]
    internal_links = main_result.get("links", [])[:max_pages - 1]
    for link in internal_links:
        try:
            link_result = await crawl4ai_crawl(link, depth=1, max_pages=1)
            if link_result.get("success") and link_result.get("content"):
                pages.append({"url": link, "title": link_result.get("title", ""), "content": link_result.get("content", "")[:5000]})
        except Exception as e:
            activity.logger.warning(f"Failed to crawl {link}: {e}")
    return {"pages": pages, "cost": 0.0}


@activity.defn
async def serper_company_search(domain: str) -> Dict[str, Any]:
    """Search for company news and mentions."""
    activity.logger.info(f"Searching news for: {domain}")
    news_result = await serper_search(query=f"{domain} news", search_type="news", num_results=10)
    return {"results": news_result.get("results", []), "cost": news_result.get("cost", 0.001)}


class CurationResult(BaseModel):
    sources: List[Dict[str, Any]]
    confidence: float
    key_facts: List[str]


@activity.defn
async def curate_research_sources(crawled_pages: List[Dict[str, Any]], news_articles: List[Dict[str, Any]], category: str) -> Dict[str, Any]:
    """AI curates and ranks research sources."""
    activity.logger.info(f"Curating research sources: {len(crawled_pages)} pages, {len(news_articles)} news")

    # Build context with limited size to avoid token overflow
    context_parts = []
    for page in crawled_pages[:3]:  # Limit to 3 pages
        content = page.get('content', '')[:1500]  # Shorter per page
        context_parts.append(f"[Website] {page.get('title', 'Page')}:\n{content}")
    for article in news_articles[:3]:  # Limit to 3 news
        context_parts.append(f"[News] {article.get('title', '')}:\n{article.get('snippet', '')}")
    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""Analyze research sources about a company (category: {category}).

{context}

Rate quality/relevance. Extract 3-5 key facts. Keep summaries brief (1 sentence each).
Return JSON: {{"sources": [{{"url": "...", "quality": 0.9, "relevance": 0.8, "summary": "..."}}], "confidence": 0.7, "key_facts": ["fact 1", "fact 2"]}}"""

    gateway = AIGateway()
    try:
        # Use gpt-4o-mini for reliable longer outputs (not Groq which truncates)
        result = await gateway.structured_output(prompt=prompt, response_model=CurationResult, model="quick")
        activity.logger.info(f"Curation success: {len(result.sources)} sources, {len(result.key_facts)} facts")
        return {"sources": result.sources, "confidence": result.confidence, "key_facts": result.key_facts, "cost": 0.002}
    except Exception as e:
        activity.logger.warning(f"Curation AI failed, using fallback: {e}")
        # Fallback: combine crawled pages AND news articles as sources
        fallback_sources = []
        for p in crawled_pages:
            fallback_sources.append({
                "url": p.get("url", ""),
                "content": p.get("content", "")[:2000],
                "quality": 0.7,
                "relevance": 0.8,
                "summary": p.get("title", "Website page")
            })
        for n in news_articles:
            fallback_sources.append({
                "url": n.get("url", ""),
                "content": n.get("snippet", ""),
                "quality": 0.6,
                "relevance": 0.7,
                "summary": n.get("title", "News article")
            })
        activity.logger.info(f"Fallback sources: {len(fallback_sources)}")
        return {"sources": fallback_sources, "confidence": 0.5, "key_facts": [], "cost": 0}
    finally:
        await gateway.close()


@activity.defn
async def check_research_ambiguity(sources: List[Dict[str, Any]], domain: str) -> Dict[str, Any]:
    """Check if research is ambiguous - only if truly insufficient."""
    signals = []

    # Check if we have ANY usable content
    has_content = any(
        len(s.get("content", "") or s.get("summary", "")) > 100
        for s in sources
    )

    if not sources:
        signals.append("no_sources")
    elif not has_content:
        signals.append("no_content")

    # Only ambiguous if we have zero usable data
    is_ambiguous = "no_sources" in signals or "no_content" in signals
    activity.logger.info(f"Ambiguity check: {len(sources)} sources, has_content={has_content}, ambiguous={is_ambiguous}")
    return {"is_ambiguous": is_ambiguous, "signals": signals}


class ProfileSection(BaseModel):
    """A narrative section of the company profile."""
    title: str
    content: str
    confidence: float = 0.8


class CompanyProfileOutput(BaseModel):
    """Full company profile with structured data and narrative sections."""
    # Essential
    legal_name: str
    slug: str
    website: str
    domain: str
    company_type: str

    # Short-form text
    tagline: Optional[str] = None
    short_description: Optional[str] = None

    # Structured data
    industry: Optional[str] = None
    headquarters_city: Optional[str] = None
    headquarters_country: Optional[str] = None
    founded_year: Optional[int] = None
    employee_range: Optional[str] = None

    # Contact
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None

    # Lists
    services: List[str] = []
    sectors: List[str] = []
    key_clients: List[str] = []

    # Narrative sections
    overview: Optional[str] = None
    services_section: Optional[str] = None
    team_section: Optional[str] = None
    track_record: Optional[str] = None


@activity.defn
async def generate_company_profile(sources: List[Dict[str, Any]], domain: str, category: str, app: str, jurisdiction: str) -> Dict[str, Any]:
    """Generate comprehensive company profile using AI."""
    activity.logger.info(f"Generating profile for {domain}")

    context_parts = [source.get("content", source.get("summary", ""))[:3000] for source in sources[:10]]
    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""Create a comprehensive company profile for {domain}.

RESEARCH CONTEXT:
{context}

INSTRUCTIONS:
Generate a detailed profile with these requirements:

ESSENTIAL FIELDS (all required):
- legal_name: Official company name
- slug: URL-friendly slug (lowercase, hyphens, e.g., "acme-corporation")
- website: Full website URL (https://{domain})
- domain: Just the domain ({domain})
- company_type: One of: {category}

SHORT-FORM (required):
- tagline: Compelling one-liner (10-15 words), plain text
- short_description: 2-3 sentence summary (40-60 words) for preview cards, plain text

STRUCTURED DATA (extract if found):
- industry: Primary industry/sector
- headquarters_city: City where HQ is located
- headquarters_country: Country where HQ is located
- founded_year: Year founded (number only)
- employee_range: One of: "1-10", "10-50", "50-100", "100-500", "500+"
- linkedin_url: Full LinkedIn URL if found
- twitter_url: Full Twitter/X URL if found

LISTS (include relevant items found):
- services: List of services/products offered
- sectors: Industries or sectors served
- key_clients: Notable clients or partnerships

NARRATIVE SECTIONS (write 2-4 paragraphs each, use markdown):
- overview: What the company does, value proposition, business model
- services_section: Detailed services, products, how they work
- team_section: Key executives, founders, leadership (if found)
- track_record: Notable deals, projects, achievements, results

Be factual - only include information found in the research context.
Use markdown formatting in narrative sections (bold, lists, etc.).
"""

    gateway = AIGateway()
    try:
        profile = await gateway.structured_output(
            prompt=prompt,
            response_model=CompanyProfileOutput,
            model="quality",  # Use GPT-4o for better quality
            system_prompt="You are an expert company analyst who creates comprehensive, factual profiles from research data. Write in a professional, engaging style."
        )

        # Build profile_sections dict from narrative fields
        profile_dict = profile.model_dump()
        profile_sections = {}

        if profile.overview:
            profile_sections["overview"] = {
                "title": "Overview",
                "content": profile.overview,
                "confidence": 0.9
            }

        if profile.services_section:
            profile_sections["services"] = {
                "title": "Services",
                "content": profile.services_section,
                "confidence": 0.85
            }

        if profile.team_section:
            profile_sections["team"] = {
                "title": "Team",
                "content": profile.team_section,
                "confidence": 0.8
            }

        if profile.track_record:
            profile_sections["track_record"] = {
                "title": "Track Record",
                "content": profile.track_record,
                "confidence": 0.75
            }

        # Add profile_sections to payload
        profile_dict["profile_sections"] = profile_sections
        profile_dict["section_count"] = len(profile_sections)
        profile_dict["total_content_length"] = sum(
            len(s["content"]) for s in profile_sections.values()
        )

        # Add metadata
        profile_dict["research_date"] = datetime.utcnow().isoformat()
        profile_dict["last_updated"] = datetime.utcnow().isoformat()
        profile_dict["confidence_score"] = 0.85
        profile_dict["data_sources"] = {
            "crawl4ai": {"pages": len(sources), "success": True},
            "serper": {"articles": 0, "cost": 0.001}
        }
        profile_dict["sources"] = [s.get("url", "") for s in sources if s.get("url")]

        # Legacy fields for compatibility
        profile_dict["about"] = profile.short_description or profile.overview
        profile_dict["category"] = profile.company_type
        profile_dict["headquarters"] = f"{profile.headquarters_city}, {profile.headquarters_country}" if profile.headquarters_city else None

        return {"success": True, "profile": profile_dict, "cost": 0.01}

    except Exception as e:
        activity.logger.error(f"Profile generation failed: {e}")
        return {"success": False, "error": str(e)}
    finally:
        await gateway.close()


@activity.defn
async def extract_and_process_logo(url: str, company_name: str) -> Dict[str, Any]:
    """Extract company logo (placeholder)."""
    activity.logger.info(f"Extracting logo for {company_name}")
    return {"logo_url": None, "source": "not_implemented"}


@activity.defn
async def save_company_to_neon(profile: Dict[str, Any], app: str) -> Dict[str, Any]:
    """Save company profile to Neon PostgreSQL."""
    activity.logger.info(f"Saving company: {profile.get('slug')}")

    # Extract overview from profile_sections if available
    profile_sections = profile.get("profile_sections", {})
    overview_text = None
    if "overview" in profile_sections:
        overview_text = profile_sections["overview"].get("content", "")

    # Truncate helper for varchar fields
    def truncate(text, max_len):
        if text and len(text) > max_len:
            return text[:max_len-3] + "..."
        return text

    # Only include columns that exist in the companies table
    data = {
        "slug": profile.get("slug"),
        "name": truncate(profile.get("legal_name", profile.get("slug", "Unknown")), 255),
        "app": app,
        "payload": json.dumps(profile),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        # Core text fields - meta_description has 160 char limit
        "description": profile.get("short_description") or profile.get("about"),
        "meta_description": truncate(profile.get("short_description"), 160),
        "overview": overview_text or profile.get("overview"),
        # Structured data
        "headquarters": profile.get("headquarters") or (
            f"{profile.get('headquarters_city')}, {profile.get('headquarters_country')}"
            if profile.get("headquarters_city") else None
        ),
        "logo_url": profile.get("logo_url"),
        "company_type": profile.get("company_type") or profile.get("category"),
        "founded_year": profile.get("founded_year"),
        # Status
        "status": "published",
        # Arrays
        "specializations": profile.get("services", [])[:5] if profile.get("services") else None,
        "tags": profile.get("sectors", [])[:5] if profile.get("sectors") else None,
    }

    # Remove None values
    data = {k: v for k, v in data.items() if v is not None}
    return await save_to_neon("companies", data, on_conflict="slug")


@activity.defn
async def sync_company_to_zep(company_id: str, profile: Dict[str, Any], app: str) -> Dict[str, Any]:
    """Sync company to Zep knowledge graph."""
    activity.logger.info(f"Syncing company to Zep: {company_id}")
    content = f"""Company: {profile.get('legal_name')}
Category: {profile.get('category')}
About: {profile.get('about', '')}
Services: {', '.join(profile.get('services', []))}"""
    graph_mapping = {"placement": "finance-knowledge", "relocation": "relocation", "jobs": "jobs"}
    graph_id = graph_mapping.get(app, "finance-knowledge")
    return await sync_to_zep(entity_id=company_id, entity_type="company", content=content, graph_id=graph_id, metadata={"slug": profile.get("slug")})


# ============================================
# ARTICLE ACTIVITIES
# ============================================

class KeywordResult(BaseModel):
    target_keyword: str
    volume: Optional[int] = None  # Must be a plain integer, no commas
    difficulty: Optional[float] = None
    secondary_keywords: List[str] = []


@activity.defn
async def research_keywords(topic: str, jurisdiction: str) -> Dict[str, Any]:
    """Research keywords for the topic."""
    activity.logger.info(f"Researching keywords for: {topic}")
    gateway = AIGateway()
    try:
        prompt = f"""Suggest SEO keywords for an article about: {topic}
Target jurisdiction: {jurisdiction}

Return JSON with:
- target_keyword: main keyword phrase
- secondary_keywords: ["keyword1", "keyword2", "keyword3"] (3-5 related keywords)
- volume: estimated monthly search volume as INTEGER (e.g., 2500 NOT "2,500")
- difficulty: SEO difficulty 0.0 to 1.0

IMPORTANT: volume must be a plain integer without commas."""

        # Use GPT-4o-mini for reliable JSON formatting
        result = await gateway.structured_output(prompt=prompt, response_model=KeywordResult, model="quick")
        activity.logger.info(f"Keyword research success: {result.target_keyword}")
        return {
            "target_keyword": result.target_keyword,
            "volume": result.volume,
            "difficulty": result.difficulty,
            "secondary_keywords": result.secondary_keywords
        }
    except Exception as e:
        activity.logger.warning(f"Keyword research failed: {e}")
        # Fallback: use topic as keyword
        fallback_keyword = topic.lower().replace(" ", "-")[:50]
        return {
            "target_keyword": fallback_keyword,
            "secondary_keywords": [],
            "volume": None,
            "difficulty": None
        }
    finally:
        await gateway.close()


@activity.defn
async def crawl_topic_urls(topic: str, max_pages: int = 10) -> Dict[str, Any]:
    """Crawl URLs related to the topic."""
    activity.logger.info(f"Crawling topic URLs for: {topic}")
    search_result = await serper_search(query=topic, search_type="search", num_results=max_pages)
    pages = []
    urls = [r.get("link") for r in search_result.get("results", []) if r.get("link")]
    for url in urls[:max_pages]:
        try:
            crawl_result = await crawl4ai_crawl(url, depth=1, max_pages=1)
            if crawl_result.get("success"):
                pages.append({"url": url, "title": crawl_result.get("title", ""), "content": crawl_result.get("content", "")[:5000]})
        except Exception as e:
            activity.logger.warning(f"Failed to crawl {url}: {e}")
    return {"pages": pages, "cost": 0.0}


@activity.defn
async def search_news(topic: str, num_results: int = 10) -> Dict[str, Any]:
    """Search for news articles about the topic."""
    activity.logger.info(f"Searching news for: {topic}")
    result = await serper_search(query=topic, search_type="news", num_results=num_results)
    return {"articles": result.get("results", []), "cost": result.get("cost", 0.001)}


class SourceCuration(BaseModel):
    sources: List[Dict[str, Any]]
    confidence: float
    key_facts: List[str]  # Simple list of fact strings
    perspectives: List[str]  # Simple list of perspective strings
    outline: List[str]  # Simple list of section titles


@activity.defn
async def curate_article_sources(crawled_pages: List[Dict[str, Any]], news_articles: List[Dict[str, Any]], topic: str) -> Dict[str, Any]:
    """AI curates and ranks research sources for article writing."""
    activity.logger.info(f"Curating article sources: {len(crawled_pages)} pages, {len(news_articles)} news")

    context_parts = []
    for page in crawled_pages[:3]:
        context_parts.append(f"[Page] {page.get('title', 'Untitled')}:\n{page.get('content', '')[:1500]}")
    for article in news_articles[:3]:
        context_parts.append(f"[News] {article.get('title', '')}:\n{article.get('snippet', '')}")
    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""Analyze research sources for an article about: {topic}

{context}

Return JSON with:
- sources: [{{"url": "...", "quality": 0.8, "relevance": 0.9, "summary": "..."}}]
- confidence: 0.8 (number)
- key_facts: ["Fact 1", "Fact 2", "Fact 3"] (simple strings)
- perspectives: ["Perspective 1", "Perspective 2"] (simple strings)
- outline: ["Introduction", "Main Point 1", "Main Point 2", "Conclusion"] (simple section titles)

IMPORTANT: key_facts, perspectives, and outline must be simple strings, NOT objects."""

    gateway = AIGateway()
    try:
        # Use GPT-4o-mini for better structured output (Groq truncates)
        result = await gateway.structured_output(prompt=prompt, response_model=SourceCuration, model="quick")
        activity.logger.info(f"Curation success: {len(result.sources)} sources")
        return {
            "sources": result.sources,
            "confidence": result.confidence,
            "key_facts": result.key_facts,
            "perspectives": result.perspectives,
            "outline": result.outline,
            "cost": 0.002
        }
    except Exception as e:
        activity.logger.warning(f"Curation failed, using fallback: {e}")
        # Fallback: combine crawled pages AND news as sources
        fallback_sources = []
        for p in crawled_pages:
            fallback_sources.append({
                "url": p.get("url", ""),
                "content": p.get("content", "")[:2000],
                "quality": 0.7,
                "summary": p.get("title", "")
            })
        for n in news_articles:
            fallback_sources.append({
                "url": n.get("url", ""),
                "content": n.get("snippet", ""),
                "quality": 0.6,
                "summary": n.get("title", "")
            })
        return {
            "sources": fallback_sources,
            "confidence": 0.5,
            "key_facts": [],
            "perspectives": [],
            "outline": ["Introduction", "Background", "Analysis", "Conclusion"],
            "cost": 0
        }
    finally:
        await gateway.close()


@activity.defn
async def get_zep_context(topic: str, app: str) -> Dict[str, Any]:
    """Query Zep knowledge graph for related context."""
    activity.logger.info(f"Querying Zep for context: {topic}")
    graph_mapping = {"placement": "finance-knowledge", "relocation": "relocation", "jobs": "jobs"}
    return {"related_companies": [], "related_facts": [], "existing_articles": [], "graph_id": graph_mapping.get(app, "finance-knowledge")}


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


@activity.defn
async def generate_four_act_article(topic: str, sources: List[Dict[str, Any]], key_facts: List[str], outline: List[str], word_count: int, target_keyword: Optional[str], secondary_keywords: List[str], app: str, article_type: str) -> Dict[str, Any]:
    """Generate a 4-act article using AI."""
    activity.logger.info(f"Generating 4-act article for: {topic}")
    context_parts = [source.get("content", source.get("summary", ""))[:1500] for source in sources[:8]]
    research_context = "\n\n".join(context_parts)
    facts_text = "\n".join(f"- {fact}" for fact in key_facts[:10])
    outline_text = "\n".join(f"{i+1}. {section}" for i, section in enumerate(outline[:4]))

    prompt = f"""Write a professional {word_count}-word article about: {topic}

RESEARCH CONTEXT:
{research_context}

KEY FACTS:
{facts_text}

SUGGESTED OUTLINE:
{outline_text}

TARGET KEYWORD: {target_keyword or topic}
SECONDARY KEYWORDS: {', '.join(secondary_keywords[:5])}

Write exactly 4 sections. Each section ~{word_count // 4} words.
Include factoid and 45-55 word visual_hint for each section.
Return: title, slug, excerpt, sections (4 with title, content, factoid, visual_hint)."""

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(prompt=prompt, response_model=GeneratedArticle, model="quick", system_prompt="You are a professional journalist.")
        html_parts = [f"<h1>{result.title}</h1>", f"<p class='excerpt'>{result.excerpt}</p>"]
        for section in result.sections:
            html_parts.extend([f"<h2>{section.title}</h2>", f"<div>{section.content}</div>"])
            if section.factoid:
                html_parts.append(f"<aside>{section.factoid}</aside>")
        content_html = "\n".join(html_parts)
        text_only = re.sub(r'<[^>]+>', '', content_html)
        return {
            "success": True,
            "article": {"title": result.title, "slug": result.slug, "excerpt": result.excerpt, "content": content_html, "sections": [s.model_dump() for s in result.sections], "word_count": len(text_only.split()), "target_keyword": target_keyword, "secondary_keywords": secondary_keywords},
            "four_act_content": [{"act_number": i + 1, "title": s.title, "factoid": s.factoid, "visual_hint": s.visual_hint} for i, s in enumerate(result.sections)],
            "cost": 0.01,
        }
    except Exception as e:
        activity.logger.error(f"Article generation failed: {e}")
        return {"success": False, "error": str(e)}
    finally:
        await gateway.close()


@activity.defn
async def save_article_to_neon(article: Dict[str, Any], four_act_content: List[Dict[str, Any]], app: str, research_sources: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Save article to Neon PostgreSQL."""
    activity.logger.info(f"Saving article: {article.get('slug')}")

    # Build payload with all article data
    payload = {
        "sections": article.get("sections", []),
        "four_act_content": four_act_content,
        "target_keyword": article.get("target_keyword"),
        "secondary_keywords": article.get("secondary_keywords", []),
        "research_sources": [s.get("url") for s in research_sources[:10]],
    }

    data = {
        "slug": article.get("slug"),
        "title": article.get("title"),
        "excerpt": article.get("excerpt"),
        "meta_description": (article.get("excerpt") or "")[:160],
        "content": article.get("content"),
        "app": app,
        "status": "draft",
        "word_count": article.get("word_count"),
        "target_keyword": article.get("target_keyword"),
        "payload": json.dumps(payload),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    # Remove None values
    data = {k: v for k, v in data.items() if v is not None}
    return await save_to_neon("articles", data, on_conflict="slug")


@activity.defn
async def sync_article_to_zep(article_id: str, article: Dict[str, Any], key_facts: List[str], app: str) -> Dict[str, Any]:
    """Sync article to Zep knowledge graph."""
    activity.logger.info(f"Syncing article to Zep: {article_id}")
    content = f"""Article: {article.get('title')}
Excerpt: {article.get('excerpt', '')}
Key Facts:
{chr(10).join('- ' + fact for fact in key_facts[:10])}"""
    graph_mapping = {"placement": "finance-knowledge", "relocation": "relocation", "jobs": "jobs"}
    return await sync_to_zep(entity_id=article_id, entity_type="article", content=content, graph_id=graph_mapping.get(app, "finance-knowledge"), metadata={"slug": article.get("slug"), "title": article.get("title")})


class VideoPrompt(BaseModel):
    prompt: str
    acts: int = 4
    duration: int = 12


@activity.defn
async def generate_video_prompt(four_act_content: List[Dict[str, Any]], app: str, character_style: Optional[str] = None) -> Dict[str, Any]:
    """Assemble 4-act video prompt from visual hints."""
    activity.logger.info("Generating video prompt")
    if not four_act_content or len(four_act_content) < 4:
        return {"success": False, "error": "Missing 4-act content"}
    acts = [f"Act {i+1} (seconds {i*3}-{(i+1)*3}): {act.get('visual_hint', '')}" for i, act in enumerate(four_act_content[:4]) if act.get("visual_hint")]
    if len(acts) < 4:
        return {"success": False, "error": "Not enough visual hints"}
    prompt = "\n\n".join(acts)
    if character_style:
        prompt = f"Style: {character_style}\n\n{prompt}"
    return {"success": True, "prompt": prompt, "acts": 4, "duration": 12}


@activity.defn
async def generate_seedance_video(prompt: str, video_quality: str = "medium") -> Dict[str, Any]:
    """Generate 4-act video using Seedance (placeholder)."""
    activity.logger.info("Generating Seedance video")
    return {"success": False, "video_url": None, "error": "Seedance integration not implemented", "cost": 0}


@activity.defn
async def upload_to_mux(video_url: str, article_id: str, app: str) -> Dict[str, Any]:
    """Upload video to MUX (placeholder)."""
    activity.logger.info(f"Uploading video to MUX for article: {article_id}")
    return {"success": False, "playback_id": None, "asset_id": None, "error": "MUX integration not implemented"}


@activity.defn
async def build_video_narrative(playback_id: str, four_act_content: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build video narrative JSON."""
    activity.logger.info(f"Building video narrative for: {playback_id}")
    return {
        "playback_id": playback_id,
        "acts": {"act_1": {"start": 0, "end": 3}, "act_2": {"start": 3, "end": 6}, "act_3": {"start": 6, "end": 9}, "act_4": {"start": 9, "end": 12}},
        "thumbnails": {f"act_{i+1}": f"https://image.mux.com/{playback_id}/thumbnail.jpg?time={i*3+1.5}" for i in range(4)},
    }


@activity.defn
async def update_article_with_video(article_id: str, playback_id: str, asset_id: str, video_narrative: Dict[str, Any]) -> Dict[str, Any]:
    """Update article with video metadata."""
    activity.logger.info(f"Updating article {article_id} with video")
    return {"success": True, "article_id": article_id, "status": "published"}
