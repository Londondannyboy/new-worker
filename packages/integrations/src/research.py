"""
Research Integrations

Crawl4AI and Serper for web research.
"""

import os
import httpx
from typing import Optional, List, Dict, Any
import structlog

logger = structlog.get_logger()


async def crawl4ai_crawl(
    url: str,
    depth: int = 1,
    max_pages: int = 10,
) -> Dict[str, Any]:
    """
    Crawl a URL using Crawl4AI.

    Args:
        url: URL to crawl
        depth: Crawl depth (1 = single page, >1 = follow links)
        max_pages: Maximum pages to crawl

    Returns:
        Dict with markdown content, links, metadata
    """
    crawl4ai_url = os.getenv("CRAWL4AI_URL") or os.getenv("CRAWL4AI_SERVICE_URL")

    if not crawl4ai_url:
        # Fallback: use httpx to fetch the page directly
        logger.warning("crawl4ai_url_not_set", url=url, fallback="httpx")
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                if response.is_success:
                    # Basic text extraction
                    text = response.text
                    # Remove script/style tags roughly
                    import re
                    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
                    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
                    text = re.sub(r'<[^>]+>', ' ', text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    return {
                        "success": True,
                        "url": url,
                        "content": text[:10000],
                        "html": response.text[:5000],
                        "title": "",
                        "links": [],
                        "external_links": [],
                    }
        except Exception as e:
            logger.error("httpx_fallback_error", error=str(e))
        return {"success": False, "error": "CRAWL4AI_URL not set and httpx fallback failed", "content": "", "links": []}

    logger.info("crawl4ai_crawl", url=url, depth=depth)

    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            f"{crawl4ai_url}/crawl",
            json={
                "urls": [url],
                "max_depth": depth,
                "max_pages": max_pages,
                "include_links": True,
                "word_count_threshold": 50,
            },
        )

        if not response.is_success:
            logger.error("crawl4ai_error", status=response.status_code)
            return {"success": False, "error": response.text, "content": "", "links": []}

        data = response.json()

        # Extract first result
        result = data.get("results", [{}])[0] if data.get("results") else {}

        return {
            "success": True,
            "url": url,
            "content": result.get("markdown", ""),
            "html": result.get("html", ""),
            "title": result.get("metadata", {}).get("title", ""),
            "links": result.get("links", {}).get("internal", [])[:20],
            "external_links": result.get("links", {}).get("external", [])[:10],
        }


async def serper_search(
    query: str,
    search_type: str = "search",  # search, news, images
    num_results: int = 10,
    location: str = "uk",
) -> Dict[str, Any]:
    """
    Search using Serper API.

    Args:
        query: Search query
        search_type: Type of search (search, news, images)
        num_results: Number of results
        location: Geographic location

    Returns:
        Dict with search results
    """
    api_key = os.getenv("SERPER_API_KEY")
    if not api_key:
        return {"success": False, "error": "SERPER_API_KEY not set", "results": []}

    logger.info("serper_search", query=query, type=search_type)

    endpoint = f"https://google.serper.dev/{search_type}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            endpoint,
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={
                "q": query,
                "gl": location,
                "num": num_results,
            },
        )

        if not response.is_success:
            logger.error("serper_error", status=response.status_code)
            return {"success": False, "error": response.text, "results": []}

        data = response.json()

        # Normalize results based on search type
        if search_type == "news":
            results = data.get("news", [])
        elif search_type == "images":
            results = data.get("images", [])
        else:
            results = data.get("organic", [])

        return {
            "success": True,
            "query": query,
            "results": [
                {
                    "url": r.get("link", ""),
                    "title": r.get("title", ""),
                    "snippet": r.get("snippet", ""),
                    "date": r.get("date", ""),
                }
                for r in results
            ],
            "cost": 0.001,  # ~$0.001 per search
        }


async def dataforseo_keyword_research(
    keyword: str,
    location: str = "UK",
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Research keywords using DataForSEO.

    Returns keyword suggestions with volume and difficulty.
    """
    login = os.getenv("DATAFORSEO_LOGIN")
    password = os.getenv("DATAFORSEO_PASSWORD")

    if not login or not password:
        return {"success": False, "error": "DataForSEO credentials not set", "keywords": []}

    logger.info("dataforseo_keyword_research", keyword=keyword, location=location)

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.dataforseo.com/v3/dataforseo_labs/google/related_keywords/live",
            auth=(login, password),
            json=[{
                "keyword": keyword,
                "location_name": location,
                "language_name": "English",
                "limit": limit,
            }],
        )

        if not response.is_success:
            return {"success": False, "error": response.text, "keywords": []}

        data = response.json()
        tasks = data.get("tasks", [{}])
        result = tasks[0].get("result", [{}])[0] if tasks else {}
        items = result.get("items", [])

        keywords = [
            {
                "keyword": item.get("keyword_data", {}).get("keyword", ""),
                "search_volume": item.get("keyword_data", {}).get("keyword_info", {}).get("search_volume", 0),
                "difficulty": item.get("keyword_data", {}).get("keyword_properties", {}).get("keyword_difficulty", 0),
                "cpc": item.get("keyword_data", {}).get("keyword_info", {}).get("cpc", 0),
            }
            for item in items[:limit]
        ]

        return {
            "success": True,
            "keyword": keyword,
            "keywords": keywords,
            "cost": 0.01,  # ~$0.01 per request
        }
