"""
Common Types and Enums

Shared across all models.
"""

from enum import Enum
from typing import Optional
from pydantic import BaseModel


class App(str, Enum):
    """Application vertical."""
    PLACEMENT = "placement"
    RELOCATION = "relocation"
    JOBS = "jobs"
    NEWSROOM = "newsroom"


class ContentType(str, Enum):
    """Content type for unified handling."""
    ARTICLE = "article"
    COMPANY = "company"
    COUNTRY = "country"
    CLUSTER = "cluster"


class ZepGraph(str, Enum):
    """Zep knowledge graph IDs."""
    FINANCE = "finance-knowledge"
    RELOCATION = "relocation"
    USERS = "users"
    JOBS = "jobs"


def get_zep_graph(app: App) -> str:
    """Get Zep graph ID for an app."""
    mapping = {
        App.PLACEMENT: ZepGraph.FINANCE.value,
        App.RELOCATION: ZepGraph.RELOCATION.value,
        App.JOBS: ZepGraph.JOBS.value,
        App.NEWSROOM: ZepGraph.FINANCE.value,
    }
    return mapping.get(app, ZepGraph.FINANCE.value)


class ResearchSource(BaseModel):
    """A research source with validation score."""
    url: str
    title: str
    content: str
    score: float = 0.0
    validation_status: str = "unknown"  # trusted, uncertain, low


class VideoNarrative(BaseModel):
    """4-Act video narrative structure."""
    playback_id: str
    asset_id: Optional[str] = None
    acts: dict = {}  # {act_1: {start: 0, end: 3}, ...}
    thumbnails: dict = {}  # {hero: url, act_1: url, ...}
