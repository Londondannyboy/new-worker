"""
Storage Integrations

Neon PostgreSQL and Zep Knowledge Graph.
"""

import os
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
import asyncpg
import httpx
import structlog

logger = structlog.get_logger()


# ============================================
# NEON DATABASE
# ============================================

async def save_to_neon(
    table: str,
    data: Dict[str, Any],
    on_conflict: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Save data to Neon PostgreSQL.

    Args:
        table: Table name
        data: Data to insert/update
        on_conflict: Column for upsert (e.g., "slug")

    Returns:
        Dict with id and success status
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"success": False, "error": "DATABASE_URL not set"}

    logger.info("save_to_neon", table=table, keys=list(data.keys()))

    conn = await asyncpg.connect(database_url)
    try:
        # Build query
        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        values = list(data.values())

        if on_conflict:
            # Upsert
            update_cols = [f"{c} = EXCLUDED.{c}" for c in columns if c != on_conflict]
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({on_conflict}) DO UPDATE SET {', '.join(update_cols)}
                RETURNING id
            """
        else:
            # Insert
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                RETURNING id
            """

        result = await conn.fetchrow(query, *values)
        return {
            "success": True,
            "id": str(result["id"]) if result else None,
        }

    except Exception as e:
        logger.error("save_to_neon_error", error=str(e))
        return {"success": False, "error": str(e)}

    finally:
        await conn.close()


async def get_from_neon(
    table: str,
    where: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Get a record from Neon.

    Args:
        table: Table name
        where: WHERE conditions (column: value)

    Returns:
        Record dict or None
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None

    conn = await asyncpg.connect(database_url)
    try:
        conditions = [f"{k} = ${i+1}" for i, k in enumerate(where.keys())]
        query = f"SELECT * FROM {table} WHERE {' AND '.join(conditions)} LIMIT 1"

        result = await conn.fetchrow(query, *where.values())
        return dict(result) if result else None

    finally:
        await conn.close()


async def check_company_exists(domain: str) -> bool:
    """Check if a company with this domain already exists."""
    result = await get_from_neon("companies", {"domain": domain})
    return result is not None


# ============================================
# ZEP KNOWLEDGE GRAPH
# ============================================

def _get_zep_headers() -> Dict[str, str]:
    """Get Zep API headers."""
    api_key = os.getenv("ZEP_API_KEY")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


async def query_zep(
    query: str,
    graph_id: str = "finance-knowledge",
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Query Zep knowledge graph for context.

    Args:
        query: Search query
        graph_id: Graph to search (finance-knowledge, relocation, jobs)
        limit: Max results

    Returns:
        Dict with facts and related entities
    """
    api_key = os.getenv("ZEP_API_KEY")
    if not api_key:
        return {"success": False, "error": "ZEP_API_KEY not set", "facts": []}

    logger.info("query_zep", query=query[:50], graph=graph_id)

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"https://api.getzep.com/api/v2/graphs/{graph_id}/search",
            headers=_get_zep_headers(),
            json={
                "query": query,
                "limit": limit,
                "search_type": "similarity",
            },
        )

        if not response.is_success:
            logger.warning("query_zep_error", status=response.status_code)
            return {"success": False, "error": response.text, "facts": []}

        data = response.json()

        return {
            "success": True,
            "facts": data.get("results", []),
            "count": len(data.get("results", [])),
        }


async def sync_to_zep(
    entity_id: str,
    entity_type: str,
    content: str,
    graph_id: str = "finance-knowledge",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Sync an entity to Zep knowledge graph.

    Args:
        entity_id: Unique entity ID
        entity_type: Type (company, article, country)
        content: Text content to store
        graph_id: Target graph
        metadata: Additional metadata

    Returns:
        Dict with success status
    """
    api_key = os.getenv("ZEP_API_KEY")
    if not api_key:
        return {"success": False, "error": "ZEP_API_KEY not set"}

    logger.info("sync_to_zep", entity_id=entity_id, type=entity_type, graph=graph_id)

    # Build fact content
    fact_data = {
        "uuid": entity_id,
        "fact": content[:5000],  # Zep has content limits
        "fact_type": entity_type,
        "valid_at": datetime.utcnow().isoformat(),
        **(metadata or {}),
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"https://api.getzep.com/api/v2/graphs/{graph_id}/facts",
            headers=_get_zep_headers(),
            json=fact_data,
        )

        if not response.is_success:
            logger.warning("sync_to_zep_error", status=response.status_code)
            return {"success": False, "error": response.text}

        return {"success": True, "graph_id": graph_id}
