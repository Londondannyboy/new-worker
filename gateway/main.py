"""
Quest Gateway

FastAPI gateway for triggering Temporal workflows.
All workflow execution goes through this API.
"""

import os
import uuid
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from temporalio.client import Client, TLSConfig

# Import from packages
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()


# ============================================
# TEMPORAL CLIENT
# ============================================

temporal_client: Optional[Client] = None


async def get_temporal_client() -> Client:
    """Get or create Temporal client."""
    global temporal_client

    if temporal_client is None:
        address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
        api_key = os.getenv("TEMPORAL_API_KEY")

        if api_key:
            temporal_client = await Client.connect(
                target_host=address,
                namespace=namespace,
                tls=TLSConfig(),
                api_key=api_key,
            )
        else:
            temporal_client = await Client.connect(
                target_host=address,
                namespace=namespace,
            )

    return temporal_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI."""
    # Startup: Connect to Temporal
    await get_temporal_client()
    print("Connected to Temporal Cloud")
    yield
    # Shutdown: Close client
    global temporal_client
    if temporal_client:
        await temporal_client.close()


# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(
    title="Quest Gateway",
    description="API gateway for Quest Temporal workflows",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# REQUEST MODELS
# ============================================

class CreateCompanyRequest(BaseModel):
    """Request to create a company profile."""
    url: str
    category: str = ""
    app: str = "placement"
    jurisdiction: str = "UK"
    force_update: bool = False
    generate_video: bool = False


class CreateArticleRequest(BaseModel):
    """Request to create an article."""
    topic: str
    app: str = "placement"
    article_type: str = "standard"
    keywords: list[str] = []
    target_keyword: Optional[str] = None
    word_count: int = 2000
    jurisdiction: str = "UK"
    video_quality: str = "medium"
    video_model: str = "seedance"
    character_style: Optional[str] = None
    custom_slug: Optional[str] = None
    generate_video: bool = True


class WorkflowResponse(BaseModel):
    """Response from workflow trigger."""
    workflow_id: str
    run_id: str
    status: str = "started"


class WorkflowStatusResponse(BaseModel):
    """Response from workflow status check."""
    workflow_id: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None


# ============================================
# HEALTH ROUTES
# ============================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "quest-gateway"}


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Quest Gateway",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "create_company": "POST /workflows/company",
            "create_article": "POST /workflows/article",
            "workflow_status": "GET /workflows/{workflow_id}",
        }
    }


# ============================================
# WORKFLOW ROUTES
# ============================================

@app.post("/workflows/company", response_model=WorkflowResponse)
async def create_company(request: CreateCompanyRequest):
    """
    Trigger CreateCompanyWorkflow.

    Creates a company profile from a URL.
    """
    client = await get_temporal_client()

    workflow_id = f"company-{uuid.uuid4().hex[:8]}"

    try:
        handle = await client.start_workflow(
            "CreateCompanyWorkflow",
            {
                "url": request.url,
                "category": request.category,
                "app": request.app,
                "jurisdiction": request.jurisdiction,
                "force_update": request.force_update,
                "generate_video": request.generate_video,
            },
            id=workflow_id,
            task_queue="quest-company-queue",
        )

        return WorkflowResponse(
            workflow_id=workflow_id,
            run_id=handle.result_run_id,
            status="started",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflows/article", response_model=WorkflowResponse)
async def create_article(request: CreateArticleRequest):
    """
    Trigger CreateArticleWorkflow.

    Creates a 4-act article from a topic.
    """
    client = await get_temporal_client()

    workflow_id = f"article-{uuid.uuid4().hex[:8]}"

    try:
        handle = await client.start_workflow(
            "CreateArticleWorkflow",
            {
                "topic": request.topic,
                "app": request.app,
                "article_type": request.article_type,
                "keywords": request.keywords,
                "target_keyword": request.target_keyword,
                "word_count": request.word_count,
                "jurisdiction": request.jurisdiction,
                "video_quality": request.video_quality,
                "video_model": request.video_model,
                "character_style": request.character_style,
                "custom_slug": request.custom_slug,
                "generate_video": request.generate_video,
            },
            id=workflow_id,
            task_queue="quest-article-queue",
        )

        return WorkflowResponse(
            workflow_id=workflow_id,
            run_id=handle.result_run_id,
            status="started",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflows/{workflow_id}", response_model=WorkflowStatusResponse)
async def get_workflow_status(workflow_id: str):
    """
    Get status of a workflow.
    """
    client = await get_temporal_client()

    try:
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe()

        status = description.status.name

        result = None
        error = None

        if status == "COMPLETED":
            result = await handle.result()
        elif status == "FAILED":
            # Get error from history
            error = "Workflow failed"

        return WorkflowStatusResponse(
            workflow_id=workflow_id,
            status=status,
            result=result,
            error=error,
        )

    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Workflow not found: {e}")


# ============================================
# CONTENT WORKER ROUTES (for Railway)
# ============================================

@app.post("/content-worker/company", response_model=WorkflowResponse)
async def content_worker_create_company(request: CreateCompanyRequest):
    """
    Alias for /workflows/company.

    Used by existing Railway routing.
    """
    return await create_company(request)


@app.post("/content-worker/article", response_model=WorkflowResponse)
async def content_worker_create_article(request: CreateArticleRequest):
    """
    Alias for /workflows/article.

    Used by existing Railway routing.
    """
    return await create_article(request)


# ============================================
# RUN SERVER
# ============================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
    )
