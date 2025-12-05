"""
Content Worker

Unified Temporal worker for all content workflows:
- CreateCompanyWorkflow (quest-company-queue)
- CreateArticleWorkflow (quest-article-queue)

This worker listens on multiple queues for flexibility.
"""

import os
import asyncio
import importlib.util
from pathlib import Path
from dotenv import load_dotenv
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

# Load environment
load_dotenv()

# Add packages to path
import sys
BASE_PATH = Path(__file__).parent.parent.parent  # Go up to repo root
sys.path.insert(0, str(BASE_PATH))


def import_from_path(module_name: str, file_path: Path):
    """Import a module from a file path (handles hyphenated directories)."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# Import company worker modules
company_workflows = import_from_path(
    "company_workflows",
    BASE_PATH / "apps" / "company-worker" / "src" / "workflows.py"
)
company_activities = import_from_path(
    "company_activities",
    BASE_PATH / "apps" / "company-worker" / "src" / "activities.py"
)

# Import article worker modules
article_workflows = import_from_path(
    "article_workflows",
    BASE_PATH / "apps" / "article-worker" / "src" / "workflows.py"
)
article_activities = import_from_path(
    "article_activities",
    BASE_PATH / "apps" / "article-worker" / "src" / "activities.py"
)

# Extract workflows
CreateCompanyWorkflow = company_workflows.CreateCompanyWorkflow
CreateArticleWorkflow = article_workflows.CreateArticleWorkflow

# Extract all activities
COMPANY_ACTIVITIES = [
    company_activities.normalize_company_url,
    company_activities.check_company_exists,
    company_activities.crawl4ai_deep_crawl,
    company_activities.serper_company_search,
    company_activities.curate_research_sources,
    company_activities.check_research_ambiguity,
    company_activities.generate_company_profile,
    company_activities.extract_and_process_logo,
    company_activities.save_company_to_neon,
    company_activities.sync_company_to_zep,
]

ARTICLE_ACTIVITIES = [
    article_activities.research_keywords,
    article_activities.crawl_topic_urls,
    article_activities.search_news,
    article_activities.curate_article_sources,
    article_activities.get_zep_context,
    article_activities.generate_four_act_article,
    article_activities.save_article_to_neon,
    article_activities.sync_article_to_zep,
    article_activities.generate_video_prompt,
    article_activities.generate_seedance_video,
    article_activities.upload_to_mux,
    article_activities.build_video_narrative,
    article_activities.update_article_with_video,
]

ALL_ACTIVITIES = COMPANY_ACTIVITIES + ARTICLE_ACTIVITIES

# Queue configuration
TASK_QUEUES = [
    "quest-content-queue",  # Main unified queue
    "quest-company-queue",  # Company-specific
    "quest-article-queue",  # Article-specific
]


async def get_temporal_client() -> Client:
    """Get Temporal client connected to Temporal Cloud."""
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    api_key = os.getenv("TEMPORAL_API_KEY")

    if api_key:
        return await Client.connect(
            target_host=address,
            namespace=namespace,
            tls=TLSConfig(),
            api_key=api_key,
        )
    else:
        return await Client.connect(
            target_host=address,
            namespace=namespace,
        )


async def run_worker(client: Client, task_queue: str):
    """Run a worker for a specific task queue."""
    print(f"  Starting worker on queue: {task_queue}")

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[
            CreateCompanyWorkflow,
            CreateArticleWorkflow,
        ],
        activities=ALL_ACTIVITIES,
    )

    await worker.run()


async def main():
    """Run the content worker on all queues."""
    print("=" * 50)
    print("Content Worker (Unified)")
    print("=" * 50)
    print(f"Queues: {TASK_QUEUES}")
    print(f"Workflows: CreateCompanyWorkflow, CreateArticleWorkflow")
    print(f"Activities: {len(ALL_ACTIVITIES)}")
    print("=" * 50)

    client = await get_temporal_client()
    print("Connected to Temporal Cloud")

    # Run workers for all queues concurrently
    await asyncio.gather(
        *[run_worker(client, queue) for queue in TASK_QUEUES]
    )


if __name__ == "__main__":
    asyncio.run(main())
