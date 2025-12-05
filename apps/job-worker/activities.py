"""
Job Worker Activities

All activities for job scraping workflows.
Uses shared packages for AI, storage, and research.
"""

import os
import re
import json
import httpx
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from temporalio import activity
from pydantic import BaseModel

# Add packages to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from packages.ai.src.gateway import AIGateway
from packages.integrations.src.research import crawl4ai_crawl
from packages.integrations.src.storage import save_to_neon, sync_to_zep


# ============================================
# PYDANTIC MODELS
# ============================================

class JobSkill(BaseModel):
    name: str
    importance: str  # essential, beneficial, bonus
    category: str  # technical, soft, domain, tool


class ExtractedSkills(BaseModel):
    skills: List[JobSkill]


# ============================================
# DATABASE ACTIVITIES
# ============================================

@activity.defn
async def get_companies_to_scrape(app: str = "jobs") -> Dict[str, Any]:
    """Get active job boards to scrape from database."""
    import asyncpg

    activity.logger.info(f"Getting companies to scrape for: {app}")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"success": False, "companies": [], "error": "DATABASE_URL not set"}

    try:
        conn = await asyncpg.connect(database_url)
        try:
            rows = await conn.fetch("""
                SELECT id, name, careers_url as board_url, board_type, vertical
                FROM job_boards
                WHERE is_active = true
                ORDER BY name
            """)

            companies = [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "board_url": row["board_url"],
                    "board_type": row["board_type"] or "unknown",
                    "vertical": row["vertical"] or "tech",
                }
                for row in rows
            ]

            activity.logger.info(f"Found {len(companies)} active job boards")
            return {"success": True, "companies": companies}

        finally:
            await conn.close()

    except Exception as e:
        activity.logger.error(f"Failed to get companies: {e}")
        return {"success": False, "companies": [], "error": str(e)}


@activity.defn
async def save_jobs_to_database(data: Dict[str, Any]) -> Dict[str, Any]:
    """Save scraped jobs to Neon database."""
    import asyncpg

    company = data["company"]
    jobs = data["jobs"]

    activity.logger.info(f"Saving {len(jobs)} jobs for {company['name']}")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"success": False, "error": "DATABASE_URL not set"}

    added = 0
    updated = 0
    errors = []

    try:
        conn = await asyncpg.connect(database_url)
        try:
            # Get or create job_board_id
            board_id = await conn.fetchval(
                "SELECT id FROM job_boards WHERE name = $1",
                company["name"]
            )

            if not board_id:
                board_id = await conn.fetchval("""
                    INSERT INTO job_boards (name, careers_url, board_type, is_active)
                    VALUES ($1, $2, $3, true)
                    RETURNING id
                """, company["name"], company.get("board_url", ""), company.get("board_type", "unknown"))

            for job in jobs:
                try:
                    existing = await conn.fetchval("""
                        SELECT id FROM jobs
                        WHERE job_board_id = $1 AND (url = $2 OR title = $3)
                    """, board_id, job.get("url"), job.get("title"))

                    if existing:
                        await conn.execute("""
                            UPDATE jobs SET
                                description = $1, department = $2, location = $3,
                                employment_type = $4, skills = $5, updated_at = NOW()
                            WHERE id = $6
                        """,
                            job.get("description"),
                            job.get("department"),
                            job.get("location"),
                            job.get("employment_type"),
                            json.dumps(job.get("skills", [])),
                            existing
                        )
                        updated += 1
                    else:
                        await conn.execute("""
                            INSERT INTO jobs (
                                job_board_id, title, description, department,
                                location, employment_type, url, skills, posted_date, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
                        """,
                            board_id,
                            job.get("title"),
                            job.get("description"),
                            job.get("department"),
                            job.get("location"),
                            job.get("employment_type"),
                            job.get("url"),
                            json.dumps(job.get("skills", [])),
                            job.get("posted_date") or datetime.utcnow()
                        )
                        added += 1

                except Exception as e:
                    errors.append(f"Job '{job.get('title')}': {str(e)}")

            activity.logger.info(f"Jobs saved: {added} added, {updated} updated")
            return {"success": True, "added": added, "updated": updated, "errors": errors}

        finally:
            await conn.close()

    except Exception as e:
        activity.logger.error(f"Failed to save jobs: {e}")
        return {"success": False, "added": 0, "updated": 0, "errors": [str(e)]}


@activity.defn
async def sync_jobs_to_zep(jobs: List[Dict[str, Any]], company_name: str) -> Dict[str, Any]:
    """Sync jobs to Zep knowledge graph using shared storage module."""
    activity.logger.info(f"Syncing {len(jobs)} jobs to Zep for {company_name}")

    synced = 0
    for job in jobs[:20]:  # Limit to 20 jobs per sync
        content = f"""Job: {job.get('title')}
Company: {company_name}
Location: {job.get('location', 'Not specified')}
Department: {job.get('department', 'Not specified')}
Skills: {', '.join([s.get('name', '') for s in job.get('skills', [])[:5]])}"""

        result = await sync_to_zep(
            entity_id=f"job-{company_name}-{job.get('title', '')[:30]}",
            entity_type="job",
            content=content,
            graph_id="jobs",
            metadata={"company": company_name, "title": job.get("title")}
        )
        if result.get("success"):
            synced += 1

    activity.logger.info(f"Synced {synced} jobs to Zep")
    return {"success": True, "synced": synced}


# ============================================
# SCRAPING ACTIVITIES
# ============================================

@activity.defn
async def scrape_greenhouse_jobs(company: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape jobs from Greenhouse via public API."""
    activity.logger.info(f"Scraping Greenhouse: {company['name']}")

    board_url = company.get("board_url", "")
    board_token = board_url.rstrip("/").split("/")[-1]

    if not board_token:
        return {"success": False, "jobs": [], "error": "No board token found"}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs",
                params={"content": "true"}
            )
            response.raise_for_status()
            data = response.json()

        jobs = []
        for job in data.get("jobs", []):
            location = job.get("location", {})
            location_name = location.get("name") if isinstance(location, dict) else str(location) if location else None

            jobs.append({
                "title": job.get("title"),
                "company_name": company["name"],
                "department": job.get("departments", [{}])[0].get("name") if job.get("departments") else None,
                "location": location_name,
                "employment_type": None,
                "description": job.get("content"),
                "url": job.get("absolute_url"),
            })

        activity.logger.info(f"Found {len(jobs)} Greenhouse jobs")
        return {"success": True, "jobs": jobs}

    except Exception as e:
        activity.logger.error(f"Greenhouse scrape failed: {e}")
        return {"success": False, "jobs": [], "error": str(e)}


@activity.defn
async def scrape_lever_jobs(company: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape jobs from Lever via public API."""
    activity.logger.info(f"Scraping Lever: {company['name']}")

    board_url = company.get("board_url", "")
    board_token = board_url.rstrip("/").split("/")[-1]

    if not board_token:
        return {"success": False, "jobs": [], "error": "No board token found"}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(f"https://api.lever.co/v0/postings/{board_token}")
            response.raise_for_status()
            data = response.json()

        jobs = []
        for job in data:
            categories = job.get("categories", {})
            jobs.append({
                "title": job.get("text"),
                "company_name": company["name"],
                "department": categories.get("department"),
                "location": categories.get("location"),
                "employment_type": categories.get("commitment"),
                "description": job.get("descriptionPlain"),
                "url": job.get("hostedUrl"),
            })

        activity.logger.info(f"Found {len(jobs)} Lever jobs")
        return {"success": True, "jobs": jobs}

    except Exception as e:
        activity.logger.error(f"Lever scrape failed: {e}")
        return {"success": False, "jobs": [], "error": str(e)}


@activity.defn
async def scrape_ashby_jobs(company: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape jobs from Ashby board using Crawl4AI."""
    activity.logger.info(f"Scraping Ashby: {company['name']}")

    board_url = company.get("board_url", "")
    if not board_url:
        return {"success": False, "jobs": [], "error": "No board URL"}

    # Use shared crawl4ai_crawl function
    crawl_result = await crawl4ai_crawl(board_url, depth=1, max_pages=1)

    if not crawl_result.get("success"):
        return {"success": False, "jobs": [], "error": crawl_result.get("error", "Crawl failed")}

    # Parse job listings from content (basic extraction)
    content = crawl_result.get("content", "")
    jobs = []

    # For Ashby, we'd need specific selectors - this is a simplified version
    # Real implementation would use CSS extraction config
    activity.logger.info(f"Crawled Ashby page, content length: {len(content)}")

    return {"success": True, "jobs": jobs, "raw_content": content[:2000]}


@activity.defn
async def scrape_generic_jobs(company: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback scraper using AI extraction via Crawl4AI."""
    activity.logger.info(f"Scraping generic: {company['name']}")

    board_url = company.get("board_url", "")
    if not board_url:
        return {"success": False, "jobs": [], "error": "No board URL"}

    # First crawl the page
    crawl_result = await crawl4ai_crawl(board_url, depth=1, max_pages=1)

    if not crawl_result.get("success"):
        return {"success": False, "jobs": [], "error": crawl_result.get("error", "Crawl failed")}

    content = crawl_result.get("content", "")
    if not content:
        return {"success": False, "jobs": [], "error": "No content found"}

    # Use AI to extract job listings
    gateway = AIGateway()
    try:
        prompt = f"""Extract job listings from this careers page content.

Company: {company['name']}
Content: {content[:4000]}

Return JSON array of jobs. Each job should have: title, department, location, url (if found).
Example: [{{"title": "Software Engineer", "department": "Engineering", "location": "London", "url": null}}]

Return ONLY a JSON array, no explanation."""

        response = await gateway.completion(prompt=prompt, model="quick")

        # Parse JSON from response
        try:
            jobs_data = json.loads(response)
            if isinstance(jobs_data, list):
                jobs = [
                    {
                        "title": j.get("title"),
                        "company_name": company["name"],
                        "department": j.get("department"),
                        "location": j.get("location"),
                        "employment_type": j.get("employment_type"),
                        "description": j.get("description"),
                        "url": j.get("url"),
                    }
                    for j in jobs_data
                ]
                activity.logger.info(f"AI extracted {len(jobs)} jobs")
                return {"success": True, "jobs": jobs}
        except json.JSONDecodeError:
            pass

        return {"success": False, "jobs": [], "error": "Failed to parse AI response"}

    except Exception as e:
        activity.logger.error(f"Generic scrape failed: {e}")
        return {"success": False, "jobs": [], "error": str(e)}
    finally:
        await gateway.close()


# ============================================
# ENRICHMENT ACTIVITIES
# ============================================

SKILL_PATTERNS = {
    "essential": [r"must have[:\s]+(.+?)(?:\.|,|$)", r"required[:\s]+(.+?)(?:\.|,|$)"],
    "beneficial": [r"nice to have[:\s]+(.+?)(?:\.|,|$)", r"preferred[:\s]+(.+?)(?:\.|,|$)"],
}


def _extract_skills_regex(description: str) -> List[Dict[str, str]]:
    """Fallback regex-based skill extraction."""
    skills = []
    for importance, patterns in SKILL_PATTERNS.items():
        for pattern in patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            for match in matches:
                skills.append({"name": match.strip()[:100], "importance": importance, "category": "unknown"})
    return skills


@activity.defn
async def extract_job_skills(jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract skills from job descriptions using AI."""
    activity.logger.info(f"Extracting skills from {len(jobs)} jobs")

    gateway = AIGateway()
    enriched_jobs = []

    try:
        for job in jobs:
            description = job.get("description", "")

            if not description:
                job["skills"] = []
                enriched_jobs.append(job)
                continue

            try:
                result = await gateway.structured_output(
                    prompt=f"""Extract skills from this job description.

Job: {job.get('title', 'Unknown')}
Description: {description[:3000]}

Return skills with name, importance (essential/beneficial/bonus), category (technical/soft/domain/tool).""",
                    response_model=ExtractedSkills,
                    model="quick"
                )
                job["skills"] = [s.model_dump() for s in result.skills]

            except Exception as e:
                activity.logger.warning(f"AI extraction failed, using regex: {e}")
                job["skills"] = _extract_skills_regex(description)

            enriched_jobs.append(job)
            activity.heartbeat()

        activity.logger.info(f"Extracted skills for {len(enriched_jobs)} jobs")
        return {"success": True, "jobs": enriched_jobs}

    except Exception as e:
        activity.logger.error(f"Skill extraction failed: {e}")
        return {"success": False, "jobs": jobs, "error": str(e)}
    finally:
        await gateway.close()


@activity.defn
async def calculate_company_trends(company_names: List[str]) -> Dict[str, Any]:
    """Calculate hiring trends for companies."""
    import asyncpg
    from collections import Counter

    activity.logger.info(f"Calculating trends for {len(company_names)} companies")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"success": False, "trends": {}}

    try:
        conn = await asyncpg.connect(database_url)
        trends = {}

        try:
            for company_name in company_names:
                rows = await conn.fetch("""
                    SELECT j.title, j.department, j.location
                    FROM jobs j
                    JOIN job_boards jb ON j.job_board_id = jb.id
                    WHERE jb.name = $1
                """, company_name)

                if not rows:
                    continue

                jobs = [dict(row) for row in rows]
                dept_counts = Counter(j.get("department") for j in jobs if j.get("department"))
                loc_counts = Counter(j.get("location") for j in jobs if j.get("location"))

                total = len(jobs)
                velocity = "high" if total > 20 else "medium" if total > 5 else "low"

                trends[company_name] = {
                    "total_jobs": total,
                    "hiring_velocity": velocity,
                    "top_departments": dict(dept_counts.most_common(5)),
                    "top_locations": dict(loc_counts.most_common(5)),
                }

            return {"success": True, "trends": trends}

        finally:
            await conn.close()

    except Exception as e:
        activity.logger.error(f"Trend calculation failed: {e}")
        return {"success": False, "trends": {}}
