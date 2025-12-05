"""
Company Models

Pydantic models for company profiles and related data.
Based on the narrative profile approach with structured fields.
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, HttpUrl

from .common import App


class CompanyInput(BaseModel):
    """Input for CreateCompanyWorkflow."""
    url: str = Field(..., description="Company URL to profile")
    category: str = Field(default="", description="Company category hint")
    app: App = Field(default=App.PLACEMENT, description="Application vertical")
    jurisdiction: str = Field(default="UK", description="Geographic focus")
    force_update: bool = Field(default=False, description="Force update even if exists")
    generate_video: bool = Field(default=False, description="Generate company video")


class TeamMember(BaseModel):
    """Key team member."""
    name: str
    title: str
    linkedin_url: Optional[str] = None


class PortfolioCompany(BaseModel):
    """Portfolio company for investment firms."""
    name: str
    sector: Optional[str] = None
    status: Optional[str] = None  # current, exited


class CompanyProfile(BaseModel):
    """
    Full company profile.

    This is the AI-generated narrative profile with structured data.
    Stored as JSONB in the `payload` column.
    """

    # Core identity
    legal_name: str = Field(..., description="Official company name")
    slug: str = Field(..., description="URL-safe identifier")
    tagline: Optional[str] = Field(None, description="Company tagline/motto")

    # Classification
    category: str = Field(..., description="Primary category")
    sub_category: Optional[str] = Field(None, description="Sub-category")
    type: str = Field(default="company", description="Entity type")

    # Narrative sections (for display)
    about: str = Field(..., description="About section narrative")
    what_we_do: Optional[str] = Field(None, description="What we do narrative")
    why_us: Optional[str] = Field(None, description="Why choose us narrative")
    expertise: Optional[str] = Field(None, description="Areas of expertise")

    # Structured data
    founded_year: Optional[int] = None
    headquarters: Optional[str] = None
    employee_count: Optional[str] = None  # "50-100", "100-500", etc.
    aum: Optional[str] = None  # Assets under management for finance
    website: Optional[str] = None

    # Lists
    services: List[str] = Field(default_factory=list)
    sectors: List[str] = Field(default_factory=list)
    locations: List[str] = Field(default_factory=list)
    key_clients: List[str] = Field(default_factory=list)
    recent_deals: List[str] = Field(default_factory=list)

    # Team
    team: List[TeamMember] = Field(default_factory=list)

    # Portfolio (for investment firms)
    portfolio: List[PortfolioCompany] = Field(default_factory=list)

    # Contact
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    linkedin_url: Optional[str] = None

    # Media
    logo_url: Optional[str] = None
    video_playback_id: Optional[str] = None

    # Metadata
    app: App = App.PLACEMENT
    jurisdiction: str = "UK"
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)
    source_urls: List[str] = Field(default_factory=list)

    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CompanyResult(BaseModel):
    """Result from CreateCompanyWorkflow."""
    success: bool
    company_id: Optional[str] = None
    slug: Optional[str] = None
    domain: Optional[str] = None
    video_playback_id: Optional[str] = None
    needs_human_review: bool = False
    error: Optional[str] = None
    cost: float = 0.0
