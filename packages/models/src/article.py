"""
Article Models

Pydantic models for article creation workflow.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class ArticleSection(BaseModel):
    """A section of the article (4-Act structure)."""
    title: str
    content: str
    factoid: Optional[str] = None
    visual_hint: Optional[str] = Field(
        None,
        description="45-55 word visual description for video generation"
    )


class FourActContent(BaseModel):
    """Video-ready content for each act."""
    act_number: int = Field(ge=1, le=4)
    title: str
    factoid: Optional[str] = None
    video_title: Optional[str] = None
    visual_hint: str = Field(
        description="45-55 word visual description for Seedance video"
    )


class ArticleOutput(BaseModel):
    """AI-generated article structure."""
    title: str
    slug: str
    excerpt: str
    content: str  # Full HTML content
    sections: List[ArticleSection] = Field(min_length=4, max_length=4)
    four_act_content: List[FourActContent] = Field(min_length=4, max_length=4)
    word_count: int
    target_keyword: Optional[str] = None
    secondary_keywords: List[str] = []


class VideoNarrative(BaseModel):
    """Video metadata for article."""
    playback_id: str
    asset_id: Optional[str] = None
    acts: Dict[str, Dict[str, float]] = Field(
        default_factory=lambda: {
            "act_1": {"start": 0, "end": 3},
            "act_2": {"start": 3, "end": 6},
            "act_3": {"start": 6, "end": 9},
            "act_4": {"start": 9, "end": 12},
        }
    )
    thumbnails: Dict[str, str] = {}


class ArticleInput(BaseModel):
    """Input for CreateArticleWorkflow."""
    topic: str
    app: str = "placement"
    article_type: str = "standard"  # standard, news, guide
    keywords: List[str] = []
    target_keyword: Optional[str] = None
    word_count: int = 2000
    jurisdiction: str = "UK"
    video_quality: str = "medium"  # none, low, medium, high
    video_model: str = "seedance"
    character_style: Optional[str] = None
    custom_slug: Optional[str] = None
    generate_video: bool = True


class ArticleResult(BaseModel):
    """Result from CreateArticleWorkflow."""
    success: bool
    article_id: Optional[str] = None
    slug: Optional[str] = None
    title: Optional[str] = None
    video_playback_id: Optional[str] = None
    word_count: Optional[int] = None
    section_count: int = 4
    total_cost: float = 0.0
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class KeywordResearch(BaseModel):
    """Keyword research results."""
    target_keyword: str
    volume: Optional[int] = None
    difficulty: Optional[float] = None
    secondary_keywords: List[str] = []
    opportunity_score: Optional[float] = None


class CuratedSource(BaseModel):
    """A curated research source."""
    url: str
    title: Optional[str] = None
    quality_score: float = Field(ge=0, le=1)
    relevance_score: float = Field(ge=0, le=1)
    summary: Optional[str] = None
    key_facts: List[str] = []


class ResearchCuration(BaseModel):
    """Result of AI curation of research sources."""
    sources: List[CuratedSource]
    confidence: float = Field(ge=0, le=1)
    key_facts: List[str]
    perspectives: List[str] = []
    article_outline: List[str] = []
