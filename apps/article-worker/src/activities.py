"""
Article Worker Activities

All activities for CreateArticleWorkflow.
AI calls go through Pydantic AI Gateway.
"""

import re
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from temporalio import activity
from pydantic import BaseModel

# Import from packages
import sys
sys.path.insert(0, "/Users/dankeegan/quest/quest-py")

from packages.ai.src.gateway import AIGateway
from packages.integrations.src.research import crawl4ai_crawl, serper_search
from packages.integrations.src.storage import save_to_neon, sync_to_zep, get_from_neon


# ============================================
# PHASE 0: KEYWORD RESEARCH
# ============================================

class KeywordResult(BaseModel):
    """Keyword research result."""
    target_keyword: str
    volume: Optional[int] = None
    difficulty: Optional[float] = None
    secondary_keywords: List[str] = []


@activity.defn
async def research_keywords(topic: str, jurisdiction: str) -> Dict[str, Any]:
    """
    Research keywords for the topic.

    Uses AI to suggest keywords (DataForSEO integration TODO).
    """
    activity.logger.info(f"Researching keywords for: {topic}")

    gateway = AIGateway()
    try:
        prompt = f"""Suggest SEO keywords for an article about: {topic}

Target jurisdiction: {jurisdiction}

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

        return {
            "target_keyword": result.target_keyword,
            "volume": result.volume,
            "difficulty": result.difficulty,
            "secondary_keywords": result.secondary_keywords,
        }
    except Exception as e:
        activity.logger.warning(f"Keyword research failed: {e}")
        # Fallback: use topic as keyword
        return {
            "target_keyword": topic.lower().replace(" ", "-"),
            "secondary_keywords": [],
        }
    finally:
        await gateway.close()


# ============================================
# PHASE 1-2: RESEARCH
# ============================================

@activity.defn
async def crawl_topic_urls(topic: str, max_pages: int = 10) -> Dict[str, Any]:
    """
    Crawl URLs related to the topic.

    Uses Crawl4AI (FREE!).
    """
    activity.logger.info(f"Crawling topic URLs for: {topic}")

    # Search for relevant URLs first
    search_result = await serper_search(
        query=topic,
        search_type="search",
        num_results=max_pages,
    )

    pages = []
    urls = [r.get("link") for r in search_result.get("results", []) if r.get("link")]

    for url in urls[:max_pages]:
        try:
            crawl_result = await crawl4ai_crawl(url, depth=1, max_pages=1)
            if crawl_result.get("success"):
                pages.append({
                    "url": url,
                    "title": crawl_result.get("title", ""),
                    "content": crawl_result.get("content", "")[:5000],
                })
        except Exception as e:
            activity.logger.warning(f"Failed to crawl {url}: {e}")

    activity.logger.info(f"Crawled {len(pages)} pages")
    return {"pages": pages, "cost": 0.0}


@activity.defn
async def search_news(topic: str, num_results: int = 10) -> Dict[str, Any]:
    """
    Search for news articles about the topic.
    """
    activity.logger.info(f"Searching news for: {topic}")

    result = await serper_search(
        query=topic,
        search_type="news",
        num_results=num_results,
    )

    return {
        "articles": result.get("results", []),
        "cost": result.get("cost", 0.001),
    }


# ============================================
# PHASE 3: CURATE SOURCES
# ============================================

class SourceCuration(BaseModel):
    """AI curation of sources."""
    sources: List[Dict[str, Any]]
    confidence: float
    key_facts: List[str]
    perspectives: List[str]
    outline: List[str]


@activity.defn
async def curate_article_sources(
    crawled_pages: List[Dict[str, Any]],
    news_articles: List[Dict[str, Any]],
    topic: str,
) -> Dict[str, Any]:
    """
    AI curates and ranks research sources for article writing.
    """
    activity.logger.info("Curating article sources via AI Gateway")

    # Build context
    context_parts = []
    for page in crawled_pages[:5]:
        context_parts.append(f"[Page] {page.get('title', 'Untitled')}:\n{page.get('content', '')[:2000]}")

    for article in news_articles[:5]:
        context_parts.append(f"[News] {article.get('title', '')}:\n{article.get('snippet', '')}")

    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""Analyze these research sources for an article about: {topic}

{context}

Curate the sources:
1. Rate quality (0-1) and relevance (0-1) of each
2. Extract key facts
3. Identify different perspectives
4. Suggest an article outline

Return JSON:
{{
  "sources": [
    {{"url": "...", "quality": 0.9, "relevance": 0.8, "summary": "..."}}
  ],
  "confidence": 0.7,
  "key_facts": ["fact 1", "fact 2"],
  "perspectives": ["perspective 1"],
  "outline": ["section 1", "section 2", "section 3", "section 4"]
}}"""

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(
            prompt=prompt,
            response_model=SourceCuration,
            model="fast",
        )

        return {
            "sources": result.sources,
            "confidence": result.confidence,
            "key_facts": result.key_facts,
            "perspectives": result.perspectives,
            "outline": result.outline,
            "cost": 0.001,
        }
    except Exception as e:
        activity.logger.error(f"Curation failed: {e}")
        return {
            "sources": [{"url": p.get("url"), "content": p.get("content", "")[:2000]}
                       for p in crawled_pages],
            "confidence": 0.5,
            "key_facts": [],
            "perspectives": [],
            "outline": [],
            "cost": 0,
        }
    finally:
        await gateway.close()


# ============================================
# PHASE 4: ZEP CONTEXT
# ============================================

@activity.defn
async def get_zep_context(topic: str, app: str) -> Dict[str, Any]:
    """
    Query Zep knowledge graph for related context.
    """
    activity.logger.info(f"Querying Zep for context: {topic}")

    # Map app to graph
    graph_mapping = {
        "placement": "finance-knowledge",
        "relocation": "relocation",
        "jobs": "jobs",
    }
    graph_id = graph_mapping.get(app, "finance-knowledge")

    # TODO: Implement actual Zep query
    # For now, return empty context
    return {
        "related_companies": [],
        "related_facts": [],
        "existing_articles": [],
        "graph_id": graph_id,
    }


# ============================================
# PHASE 5: GENERATE 4-ACT ARTICLE
# ============================================

class ArticleSection(BaseModel):
    """Single article section."""
    title: str
    content: str
    factoid: Optional[str] = None
    visual_hint: str


class GeneratedArticle(BaseModel):
    """AI-generated article."""
    title: str
    slug: str
    excerpt: str
    sections: List[ArticleSection]
    target_keyword: Optional[str] = None
    secondary_keywords: List[str] = []


@activity.defn
async def generate_four_act_article(
    topic: str,
    sources: List[Dict[str, Any]],
    key_facts: List[str],
    outline: List[str],
    word_count: int,
    target_keyword: Optional[str],
    secondary_keywords: List[str],
    app: str,
    article_type: str,
) -> Dict[str, Any]:
    """
    Generate a 4-act article using AI.

    Each section includes a visual hint for video generation.
    """
    activity.logger.info(f"Generating 4-act article for: {topic}")

    # Build research context
    context_parts = []
    for source in sources[:8]:
        content = source.get("content", source.get("summary", ""))[:1500]
        context_parts.append(content)

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
ARTICLE TYPE: {article_type}
APP: {app}

REQUIREMENTS:
1. Write exactly 4 sections (4-Act structure)
2. Each section should be ~{word_count // 4} words
3. Include a compelling factoid for each section
4. Write a 45-55 word "visual_hint" for each section describing a video scene
5. Be factual - only include information from the research
6. Naturally include the target keyword and secondary keywords
7. Write in a professional, engaging tone

For visual_hint: Describe a cinematic scene that illustrates the section's key point.
Example: "A modern office building at sunset, camera slowly pans across floor-to-ceiling windows revealing a team of analysts reviewing financial charts on multiple screens, warm golden light creates professional atmosphere."

Return the article with:
- title: Compelling headline with target keyword
- slug: URL-safe lowercase (e.g., "private-equity-trends-2024")
- excerpt: 2-3 sentence summary
- sections: Array of 4 sections, each with title, content, factoid, visual_hint"""

    gateway = AIGateway()
    try:
        result = await gateway.structured_output(
            prompt=prompt,
            response_model=GeneratedArticle,
            model="quick",  # GPT-4o-mini for quality
            system_prompt="You are a professional financial journalist writing engaging, well-researched articles.",
        )

        # Build full HTML content
        html_parts = [f"<h1>{result.title}</h1>", f"<p class='excerpt'>{result.excerpt}</p>"]

        for i, section in enumerate(result.sections):
            html_parts.append(f"<h2>{section.title}</h2>")
            html_parts.append(f"<div class='section-content'>{section.content}</div>")
            if section.factoid:
                html_parts.append(f"<aside class='factoid'>{section.factoid}</aside>")

        content_html = "\n".join(html_parts)

        # Calculate word count
        text_only = re.sub(r'<[^>]+>', '', content_html)
        actual_word_count = len(text_only.split())

        return {
            "success": True,
            "article": {
                "title": result.title,
                "slug": result.slug,
                "excerpt": result.excerpt,
                "content": content_html,
                "sections": [s.model_dump() for s in result.sections],
                "word_count": actual_word_count,
                "target_keyword": target_keyword,
                "secondary_keywords": secondary_keywords,
            },
            "four_act_content": [
                {
                    "act_number": i + 1,
                    "title": s.title,
                    "factoid": s.factoid,
                    "visual_hint": s.visual_hint,
                }
                for i, s in enumerate(result.sections)
            ],
            "cost": 0.01,
        }
    except Exception as e:
        activity.logger.error(f"Article generation failed: {e}")
        return {
            "success": False,
            "error": str(e),
        }
    finally:
        await gateway.close()


# ============================================
# PHASE 6: SAVE TO DATABASE
# ============================================

@activity.defn
async def save_article_to_neon(
    article: Dict[str, Any],
    four_act_content: List[Dict[str, Any]],
    app: str,
    research_sources: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Save article to Neon PostgreSQL.
    """
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
        "payload": json.dumps({
            "sections": article.get("sections", []),
            "four_act_content": four_act_content,
            "target_keyword": article.get("target_keyword"),
            "secondary_keywords": article.get("secondary_keywords", []),
            "research_sources": [s.get("url") for s in research_sources[:10]],
        }),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    result = await save_to_neon("articles", data, on_conflict="slug")
    return result


# ============================================
# PHASE 7: SYNC TO ZEP
# ============================================

@activity.defn
async def sync_article_to_zep(
    article_id: str,
    article: Dict[str, Any],
    key_facts: List[str],
    app: str,
) -> Dict[str, Any]:
    """
    Sync article to Zep knowledge graph.
    """
    activity.logger.info(f"Syncing article to Zep: {article_id}")

    # Build content for Zep
    content = f"""Article: {article.get('title')}

Excerpt: {article.get('excerpt', '')}

Key Facts:
{chr(10).join('- ' + fact for fact in key_facts[:10])}

Keywords: {article.get('target_keyword', '')}
"""

    # Map app to graph
    graph_mapping = {
        "placement": "finance-knowledge",
        "relocation": "relocation",
        "jobs": "jobs",
    }
    graph_id = graph_mapping.get(app, "finance-knowledge")

    result = await sync_to_zep(
        entity_id=article_id,
        entity_type="article",
        content=content,
        graph_id=graph_id,
        metadata={"slug": article.get("slug"), "title": article.get("title")},
    )

    return result


# ============================================
# PHASE 8: VIDEO PROMPT GENERATION
# ============================================

class VideoPrompt(BaseModel):
    """Generated video prompt."""
    prompt: str
    acts: int = 4
    duration: int = 12


@activity.defn
async def generate_video_prompt(
    four_act_content: List[Dict[str, Any]],
    app: str,
    character_style: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Assemble 4-act video prompt from visual hints.
    """
    activity.logger.info("Generating video prompt from 4-act content")

    if not four_act_content or len(four_act_content) < 4:
        return {"success": False, "error": "Missing 4-act content"}

    # Build prompt from visual hints
    acts = []
    for i, act in enumerate(four_act_content[:4]):
        hint = act.get("visual_hint", "")
        if hint:
            acts.append(f"Act {i+1} (seconds {i*3}-{(i+1)*3}): {hint}")

    if len(acts) < 4:
        return {"success": False, "error": "Not enough visual hints"}

    prompt = "\n\n".join(acts)

    # Add character style if provided
    if character_style:
        prompt = f"Style: {character_style}\n\n{prompt}"

    return {
        "success": True,
        "prompt": prompt,
        "acts": 4,
        "duration": 12,
    }


# ============================================
# PHASE 9-10: VIDEO GENERATION (Seedance + MUX)
# ============================================

@activity.defn
async def generate_seedance_video(
    prompt: str,
    video_quality: str = "medium",
) -> Dict[str, Any]:
    """
    Generate 4-act video using Seedance.

    TODO: Implement Seedance API call.
    """
    activity.logger.info("Generating Seedance video")

    # Placeholder - in production would call Seedance API
    return {
        "success": False,
        "video_url": None,
        "error": "Seedance integration not implemented",
        "cost": 0,
    }


@activity.defn
async def upload_to_mux(
    video_url: str,
    article_id: str,
    app: str,
) -> Dict[str, Any]:
    """
    Upload video to MUX.

    TODO: Implement MUX upload.
    """
    activity.logger.info(f"Uploading video to MUX for article: {article_id}")

    # Placeholder - in production would call MUX API
    return {
        "success": False,
        "playback_id": None,
        "asset_id": None,
        "error": "MUX integration not implemented",
    }


# ============================================
# PHASE 11: VIDEO NARRATIVE
# ============================================

@activity.defn
async def build_video_narrative(
    playback_id: str,
    four_act_content: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build video narrative JSON with act timestamps and thumbnails.
    """
    activity.logger.info(f"Building video narrative for: {playback_id}")

    narrative = {
        "playback_id": playback_id,
        "acts": {
            "act_1": {"start": 0, "end": 3},
            "act_2": {"start": 3, "end": 6},
            "act_3": {"start": 6, "end": 9},
            "act_4": {"start": 9, "end": 12},
        },
        "thumbnails": {
            "hero": f"https://image.mux.com/{playback_id}/thumbnail.jpg?time=0",
            "act_1": f"https://image.mux.com/{playback_id}/thumbnail.jpg?time=1.5",
            "act_2": f"https://image.mux.com/{playback_id}/thumbnail.jpg?time=4.5",
            "act_3": f"https://image.mux.com/{playback_id}/thumbnail.jpg?time=7.5",
            "act_4": f"https://image.mux.com/{playback_id}/thumbnail.jpg?time=10.5",
        },
    }

    return narrative


# ============================================
# PHASE 12: FINAL UPDATE
# ============================================

@activity.defn
async def update_article_with_video(
    article_id: str,
    playback_id: str,
    asset_id: str,
    video_narrative: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Update article with video metadata and publish.
    """
    activity.logger.info(f"Updating article {article_id} with video")

    # Update article in Neon
    update_data = {
        "video_playback_id": playback_id,
        "video_asset_id": asset_id,
        "video_narrative": json.dumps(video_narrative),
        "featured_asset_url": video_narrative.get("thumbnails", {}).get("hero"),
        "status": "published",
        "updated_at": datetime.utcnow(),
    }

    # TODO: Implement update in Neon

    return {
        "success": True,
        "article_id": article_id,
        "status": "published",
    }
