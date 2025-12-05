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
    activity.logger.info("Curating research sources")
    context_parts = []
    for page in crawled_pages[:5]:
        context_parts.append(f"[Website] {page.get('title', 'Page')}:\n{page.get('content', '')[:2000]}")
    for article in news_articles[:5]:
        context_parts.append(f"[News] {article.get('title', '')}:\n{article.get('snippet', '')}")
    context = "\n\n---\n\n".join(context_parts)
    prompt = f"""Analyze these research sources about a company (category: {category}).

{context}

Rate quality and relevance of each source. Extract key facts.
Return JSON: {{"sources": [{{"url": "...", "quality": 0.9, "relevance": 0.8, "summary": "..."}}], "confidence": 0.7, "key_facts": ["fact 1"]}}"""

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(prompt=prompt, response_model=CurationResult, model="fast")
        return {"sources": result.sources, "confidence": result.confidence, "key_facts": result.key_facts, "cost": 0.001}
    except Exception as e:
        activity.logger.error(f"Curation failed: {e}")
        return {"sources": [{"url": p.get("url"), "content": p.get("content", "")[:2000]} for p in crawled_pages], "confidence": 0.5, "key_facts": [], "cost": 0}
    finally:
        await gateway.close()


@activity.defn
async def check_research_ambiguity(sources: List[Dict[str, Any]], domain: str) -> Dict[str, Any]:
    """Check if research is ambiguous."""
    signals = []
    if len(sources) < 2:
        signals.append("insufficient_sources")
    return {"is_ambiguous": len(signals) > 0 and "insufficient_sources" in signals, "signals": signals}


class CompanyProfileOutput(BaseModel):
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
async def generate_company_profile(sources: List[Dict[str, Any]], domain: str, category: str, app: str, jurisdiction: str) -> Dict[str, Any]:
    """Generate company profile using AI."""
    activity.logger.info(f"Generating profile for {domain}")
    context_parts = [source.get("content", source.get("summary", ""))[:2000] for source in sources[:8]]
    context = "\n\n".join(context_parts)
    prompt = f"""Create a comprehensive company profile for {domain}.

Research Context:
{context}

Category: {category}, Jurisdiction: {jurisdiction}

Generate: legal_name, slug, tagline, about (2-3 paragraphs), what_we_do, category, services, sectors, key_clients.
Be factual - only include information from research."""

    gateway = AIGateway()
    try:
        profile = await gateway.structured_output(prompt=prompt, response_model=CompanyProfileOutput, model="quick", system_prompt="You are a professional business analyst.")
        return {"success": True, "profile": profile.model_dump(), "cost": 0.005}
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
    volume: Optional[int] = None
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
Return: target_keyword, secondary_keywords (3-5), volume (estimate), difficulty (0-1)"""
        result = await gateway.structured_output(prompt=prompt, response_model=KeywordResult, model="fast")
        return {"target_keyword": result.target_keyword, "volume": result.volume, "difficulty": result.difficulty, "secondary_keywords": result.secondary_keywords}
    except Exception as e:
        activity.logger.warning(f"Keyword research failed: {e}")
        return {"target_keyword": topic.lower().replace(" ", "-"), "secondary_keywords": []}
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
    key_facts: List[str]
    perspectives: List[str]
    outline: List[str]


@activity.defn
async def curate_article_sources(crawled_pages: List[Dict[str, Any]], news_articles: List[Dict[str, Any]], topic: str) -> Dict[str, Any]:
    """AI curates and ranks research sources for article writing."""
    activity.logger.info("Curating article sources")
    context_parts = []
    for page in crawled_pages[:5]:
        context_parts.append(f"[Page] {page.get('title', 'Untitled')}:\n{page.get('content', '')[:2000]}")
    for article in news_articles[:5]:
        context_parts.append(f"[News] {article.get('title', '')}:\n{article.get('snippet', '')}")
    context = "\n\n---\n\n".join(context_parts)
    prompt = f"""Analyze these research sources for an article about: {topic}

{context}

Curate: rate quality/relevance, extract key facts, identify perspectives, suggest 4-section outline.
Return JSON with sources, confidence, key_facts, perspectives, outline."""

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(prompt=prompt, response_model=SourceCuration, model="fast")
        return {"sources": result.sources, "confidence": result.confidence, "key_facts": result.key_facts, "perspectives": result.perspectives, "outline": result.outline, "cost": 0.001}
    except Exception as e:
        activity.logger.error(f"Curation failed: {e}")
        return {"sources": [{"url": p.get("url"), "content": p.get("content", "")[:2000]} for p in crawled_pages], "confidence": 0.5, "key_facts": [], "perspectives": [], "outline": [], "cost": 0}
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
    data = {
        "slug": article.get("slug"),
        "title": article.get("title"),
        "excerpt": article.get("excerpt"),
        "content": article.get("content"),
        "app": app,
        "type": "article",
        "status": "draft",
        "word_count": article.get("word_count"),
        "payload": json.dumps({"sections": article.get("sections", []), "four_act_content": four_act_content, "target_keyword": article.get("target_keyword"), "secondary_keywords": article.get("secondary_keywords", []), "research_sources": [s.get("url") for s in research_sources[:10]]}),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
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
